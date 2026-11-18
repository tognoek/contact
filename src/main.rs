// TUI: cài đặt (lưu settings.txt) -> cào proxy -> kiểm tra proxy sống -> mỗi proxy 1 luồng gửi GET theo chu kỳ.
use ratatui::{
    crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Cell, Gauge, List, ListItem, Paragraph, Row, Table},
    DefaultTerminal, Frame,
};
use std::{collections::{HashSet, VecDeque}, fs, sync::{Arc, Mutex}, time::{Duration, Instant}};
use tokio::{sync::Semaphore, task::{JoinError, JoinHandle, JoinSet}};

const FILE: &str = "settings.txt";
const KEYS: [&str; 8] = ["url", "streams", "interval", "scrape", "proxy_file", "timeout", "max_check", "sources"];
const LABELS: [&str; 8] = [
    "Link", "Số luồng", "Chu kỳ gửi (giây)", "Tự cào proxy (Enter đổi)",
    "File proxy thủ công", "Timeout kiểm tra (giây)", "Số proxy kiểm tra tối đa", "Nguồn proxy (cách bằng dấu phẩy)",
];
const SOURCES: &str = "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=5000&country=all&ssl=all&anonymity=all,\
https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt,\
https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt";
const RUN: &str = "Đang chạy";

// ---------- cài đặt ----------
fn load() -> Vec<String> {
    let mut v: Vec<String> = ["", "5", "600", "yes", "", "8", "1000", SOURCES].map(String::from).into();
    for l in fs::read_to_string(FILE).unwrap_or_default().lines() {
        if let Some((k, x)) = l.split_once('=') {
            if let Some(i) = KEYS.iter().position(|&q| q == k.trim()) { v[i] = x.trim().into() }
        }
    }
    v
}
fn save(v: &[String]) {
    let _ = fs::write(FILE, KEYS.iter().zip(v).map(|(k, x)| format!("{k}={x}\n")).collect::<String>());
}

struct Cfg { url: String, n: usize, secs: u64, scrape: bool, pfile: String, to: u64, max_check: usize, sources: Vec<String> }
fn parse(v: &[String]) -> Result<Cfg, String> {
    let url = v[0].trim().to_string();
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return Err("Link phải bắt đầu bằng http:// hoặc https://".into());
    }
    let num = |i: usize| v[i].trim().parse::<u64>().ok().filter(|&x| x > 0).ok_or(format!("'{}' phải là số > 0", LABELS[i]));
    Ok(Cfg {
        url, n: num(1)? as usize, secs: num(2)?, scrape: v[3] == "yes", pfile: v[4].trim().into(),
        to: num(5)?, max_check: num(6)? as usize,
        sources: v[7].split(',').map(str::trim).filter(|s| !s.is_empty()).map(String::from).collect(),
    })
}

// ---------- trạng thái chung ----------
#[derive(Default, Clone)]
struct St { ok: u32, err: u32, last: String, at: Option<Instant>, proxy: String }
#[derive(Default)]
struct Shared { phase: String, found: usize, checked: usize, total: usize, secs: u64, streams: Vec<St>, logs: VecDeque<String> }
type Sh = Arc<Mutex<Shared>>;

fn log(sh: &Sh, m: impl Into<String>) {
    let mut s = sh.lock().unwrap();
    if s.logs.len() >= 100 { s.logs.pop_front(); }
    s.logs.push_back(m.into());
}
fn phase(sh: &Sh, p: &str) { sh.lock().unwrap().phase = p.into() }
fn fail(sh: &Sh, m: String) { log(sh, m.clone()); phase(sh, &format!("✗ {m}")) }

// ---------- proxy ----------
fn norm(l: &str) -> Option<String> {
    let l = l.trim();
    if l.contains("://") { return Some(l.into()) }
    l.rsplit_once(':')?.1.parse::<u16>().ok()?; // ip:port
    Some(format!("http://{l}"))
}
async fn alive(p: &str, url: &str, to: u64) -> bool {
    let Ok(px) = reqwest::Proxy::all(p) else { return false };
    let Ok(c) = reqwest::Client::builder().proxy(px).timeout(Duration::from_secs(to)).build() else { return false };
    c.get(url).send().await.is_ok_and(|r| r.status().as_u16() < 500)
}
fn tally(sh: &Sh, good: &mut Vec<String>, r: Result<(String, bool), JoinError>) {
    if let Ok((p, ok)) = r {
        let mut s = sh.lock().unwrap();
        s.checked += 1;
        if ok { s.found += 1; good.push(p) }
    }
}

async fn run(c: Cfg, sh: Sh) {
    let mut good: Vec<String> = vec![];
    if !c.pfile.is_empty() {
        match fs::read_to_string(&c.pfile) {
            Ok(t) => {
                good.extend(t.lines().filter(|l| !l.trim_start().starts_with('#')).filter_map(norm));
                sh.lock().unwrap().found = good.len();
                log(&sh, format!("File proxy: {} proxy", good.len()));
            }
            Err(e) => return fail(&sh, format!("Không đọc được {}: {e}", c.pfile)),
        }
    }
    if c.scrape && good.len() < c.n {
        phase(&sh, "Đang cào proxy…");
        let cl = reqwest::Client::builder().timeout(Duration::from_secs(20)).build().unwrap();
        let mut cands: Vec<String> = vec![];
        for s in &c.sources {
            let host = s.split('/').nth(2).unwrap_or(s);
            match async { cl.get(s).send().await?.error_for_status()?.text().await }.await {
                Ok(t) => { let b = cands.len(); cands.extend(t.lines().filter_map(norm)); log(&sh, format!("+{} proxy từ {host}", cands.len() - b)) }
                Err(e) => log(&sh, format!("Lỗi nguồn {host}: {e}")),
            }
        }
        let mut seen = HashSet::new();
        cands.retain(|p| seen.insert(p.clone()));
        cands.truncate(c.max_check);
        sh.lock().unwrap().total = cands.len();
        phase(&sh, "Đang kiểm tra proxy…");
        let sem = Arc::new(Semaphore::new(100));
        let mut set = JoinSet::new();
        for p in cands {
            let permit = sem.clone().acquire_owned().await.unwrap();
            while let Some(r) = set.try_join_next() { tally(&sh, &mut good, r) }
            if good.len() >= c.n { break }
            let (u, to) = (c.url.clone(), c.to);
            set.spawn(async move { let ok = alive(&p, &u, to).await; drop(permit); (p, ok) });
        }
        if good.len() >= c.n { set.abort_all() }
        while let Some(r) = set.join_next().await { tally(&sh, &mut good, r) }
        log(&sh, format!("Proxy sống: {}", good.len()));
    }
    if good.is_empty() && (c.scrape || !c.pfile.is_empty()) {
        return fail(&sh, "Không có proxy nào dùng được".into());
    }
    let px: Vec<String> = (0..c.n).map(|i| good.get(i % good.len().max(1)).cloned().unwrap_or_default()).collect();
    {
        let mut s = sh.lock().unwrap();
        s.secs = c.secs;
        s.streams = px.iter().map(|p| St { proxy: p.clone(), ..Default::default() }).collect();
        s.phase = RUN.into();
    }
    let mut ws = JoinSet::new();
    for (i, p) in px.into_iter().enumerate() { ws.spawn(worker(i, p, c.url.clone(), c.secs, sh.clone())); }
    while ws.join_next().await.is_some() {}
}

async fn worker(i: usize, p: String, url: String, secs: u64, sh: Sh) {
    let mut b = reqwest::Client::builder().timeout(Duration::from_secs(30)).tcp_keepalive(Duration::from_secs(60));
    if !p.is_empty() {
        match reqwest::Proxy::all(&p) {
            Ok(px) => b = b.proxy(px),
            Err(e) => { sh.lock().unwrap().streams[i].last = format!("proxy lỗi: {e}"); return }
        }
    }
    let c = b.build().expect("client");
    let mut t = tokio::time::interval(Duration::from_secs(secs));
    loop {
        t.tick().await;
        let r = c.get(&url).send().await;
        let mut g = sh.lock().unwrap();
        let s = &mut g.streams[i];
        s.at = Some(Instant::now());
        match r {
            Ok(r) => { s.ok += 1; s.last = r.status().to_string() }
            Err(e) => { s.err += 1; s.last = e.to_string() }
        }
    }
}

// ---------- TUI ----------
enum Scr { Settings, Running }
struct App { v: Vec<String>, sel: usize, edit: bool, scr: Scr, sh: Sh, job: Option<JoinHandle<()>>, msg: String, scroll: usize }

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> std::io::Result<()> {
    let mut term = ratatui::init();
    let r = app(&mut term);
    ratatui::restore();
    r
}

fn app(term: &mut DefaultTerminal) -> std::io::Result<()> {
    let mut a = App { v: load(), sel: 0, edit: false, scr: Scr::Settings, sh: Default::default(), job: None, msg: String::new(), scroll: 0 };
    loop {
        term.draw(|f| ui(f, &a))?;
        if !event::poll(Duration::from_millis(250))? { continue }
        let Event::Key(k) = event::read()? else { continue };
        if k.kind != KeyEventKind::Press { continue }
        if k.modifiers.contains(KeyModifiers::CONTROL) && k.code == KeyCode::Char('c') { break }
        match a.scr {
            Scr::Settings if a.edit => match k.code {
                KeyCode::Enter | KeyCode::Esc => { a.edit = false; save(&a.v) }
                KeyCode::Backspace => { a.v[a.sel].pop(); }
                KeyCode::Char(c) => a.v[a.sel].push(c),
                _ => {}
            },
            Scr::Settings => match k.code {
                KeyCode::Char('q') => break,
                KeyCode::Up => a.sel = (a.sel + LABELS.len() - 1) % LABELS.len(),
                KeyCode::Down | KeyCode::Tab => a.sel = (a.sel + 1) % LABELS.len(),
                KeyCode::Enter | KeyCode::Char(' ') if a.sel == 3 => {
                    a.v[3] = if a.v[3] == "yes" { "no" } else { "yes" }.into();
                    save(&a.v);
                }
                KeyCode::Enter => a.edit = true,
                KeyCode::Char('s') | KeyCode::F(5) => match parse(&a.v) {
                    Err(e) => a.msg = e,
                    Ok(c) => {
                        save(&a.v);
                        *a.sh.lock().unwrap() = Shared::default();
                        a.job = Some(tokio::spawn(run(c, a.sh.clone())));
                        (a.scr, a.scroll, a.msg) = (Scr::Running, 0, String::new());
                    }
                },
                _ => {}
            },
            Scr::Running => match k.code {
                KeyCode::Char('q') => break,
                KeyCode::Esc => { if let Some(j) = a.job.take() { j.abort() } a.scr = Scr::Settings }
                KeyCode::Up => a.scroll = a.scroll.saturating_sub(1),
                KeyCode::Down => a.scroll = (a.scroll + 1).min(a.sh.lock().unwrap().streams.len().saturating_sub(1)),
                _ => {}
            },
        }
    }
    if let Some(j) = a.job { j.abort() }
    Ok(())
}

fn dim() -> Style { Style::new().fg(Color::DarkGray) }
fn boxed(title: String) -> Block<'static> {
    Block::bordered().border_type(BorderType::Rounded).border_style(Style::new().fg(Color::Cyan)).title(title)
}

fn ui(f: &mut Frame, a: &App) {
    let [head, body, foot] = Layout::vertical([Constraint::Length(3), Constraint::Min(0), Constraint::Length(2)]).areas(f.area());
    f.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(" ◉ CONTACT ", Style::new().fg(Color::Black).bg(Color::Cyan).add_modifier(Modifier::BOLD)),
            Span::styled("  proxy keep-alive", dim()),
        ])).block(boxed(String::new())),
        head,
    );
    match a.scr { Scr::Settings => settings(f, a, body), Scr::Running => running(f, a, body) }
    let help = match (&a.scr, a.edit) {
        (Scr::Settings, true) => "gõ để sửa · Backspace xóa · Enter/Esc xong",
        (Scr::Settings, false) => "↑↓ chọn · Enter sửa · S/F5 bắt đầu · Q thoát",
        _ => "↑↓ cuộn · Esc dừng & về cài đặt · Q thoát",
    };
    f.render_widget(
        Paragraph::new(vec![Line::styled(help, dim()), Line::styled(a.msg.clone(), Style::new().fg(Color::Red))]),
        foot,
    );
}

fn settings(f: &mut Frame, a: &App, area: Rect) {
    let items: Vec<ListItem> = LABELS.iter().enumerate().map(|(i, l)| {
        let on = i == a.sel;
        let val = if i == 3 { if a.v[3] == "yes" { "● Bật".to_string() } else { "○ Tắt".into() } } else { a.v[i].clone() };
        let cur = if on && a.edit { "▏" } else { "" };
        let st = if on { Style::new().fg(Color::Black).bg(if a.edit { Color::Yellow } else { Color::Cyan }) } else { Style::new() };
        ListItem::new(Line::from(format!("{}{l:<34} {val}{cur}", if on { " ▶ " } else { "   " }))).style(st)
    }).collect();
    f.render_widget(List::new(items).block(boxed(" Cài đặt (tự lưu vào settings.txt) ".into())), area);
}

fn running(f: &mut Frame, a: &App, area: Rect) {
    let sh = a.sh.lock().unwrap();
    let [top, mid, bot] = Layout::vertical([Constraint::Length(3), Constraint::Min(3), Constraint::Length(8)]).areas(area);
    let failed = sh.phase.starts_with('✗');
    let ratio = if sh.phase == RUN { 1.0 } else if sh.total > 0 { (sh.checked as f64 / sh.total as f64).min(1.0) } else { 0.0 };
    f.render_widget(
        Gauge::default()
            .block(boxed(format!(" {} ", sh.phase)))
            .gauge_style(Style::new().fg(if failed { Color::Red } else if sh.phase == RUN { Color::Green } else { Color::Cyan }))
            .ratio(ratio)
            .label(format!("kiểm tra {}/{}  ·  proxy sống: {}", sh.checked, sh.total, sh.found)),
        top,
    );
    let rows = sh.streams.iter().enumerate().skip(a.scroll).map(|(i, s)| {
        let next = s.at.map_or("…".into(), |t| format!("{}s", sh.secs.saturating_sub(t.elapsed().as_secs())));
        Row::new(vec![
            Cell::from(i.to_string()),
            Cell::from(s.ok.to_string()).style(Style::new().fg(Color::Green)),
            Cell::from(s.err.to_string()).style(Style::new().fg(Color::Red)),
            Cell::from(next),
            Cell::from(if s.proxy.is_empty() { "direct".into() } else { s.proxy.clone() }),
            Cell::from(s.last.clone()),
        ])
    });
    let hdr = Row::new(["#", "OK", "LỖI", "TIẾP", "PROXY", "KẾT QUẢ"]).style(Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD));
    f.render_widget(
        Table::new(rows, [Constraint::Length(4), Constraint::Length(5), Constraint::Length(5), Constraint::Length(6), Constraint::Length(32), Constraint::Min(10)])
            .header(hdr).block(boxed(format!(" Luồng ({}) ", sh.streams.len()))),
        mid,
    );
    let logs: Vec<ListItem> = sh.logs.iter().rev().take(6).rev().map(|l| ListItem::new(l.clone())).collect();
    f.render_widget(List::new(logs).block(boxed(" Nhật ký ".into())), bot);
}
