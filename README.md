# contact

Công cụ TUI nhẹ viết bằng Rust: cào proxy công khai, lọc proxy sống, rồi mở nhiều luồng gửi request GET đến một link theo chu kỳ (mặc định 10 phút/lần).

> **Chỉ phục vụ học tập.** Repo này dùng để tìm hiểu Rust async (tokio), HTTP client qua proxy (reqwest) và TUI (ratatui).
> Chỉ dùng với link/server của chính bạn hoặc được cho phép. Không dùng để tăng view/lượt truy cập giả, spam hay tạo tải lên dịch vụ của người khác. Người dùng tự chịu trách nhiệm khi sử dụng.

## Tính năng

- Giao diện TUI: màn hình cài đặt, bảng trạng thái từng luồng, nhật ký.
- Tự cào proxy từ nhiều nguồn, bỏ trùng, kiểm tra sống (tối đa 100 proxy song song) và dừng khi đủ số cần.
- Hỗ trợ thêm proxy thủ công từ file (`http://`, `socks5://`, `user:pass@host:port`).
- Mỗi luồng = 1 proxy + 1 client riêng, gửi GET theo chu kỳ tùy chỉnh.
- Cài đặt tự lưu vào `settings.txt` và đọc lại lần sau.
- Binary nhỏ (release bật `opt-level = "z"`, LTO, strip).

## Build & chạy

Cần Rust (cargo).

```
cargo run --release
```

Phím tắt:

| Màn hình | Phím | Tác dụng |
|---|---|---|
| Cài đặt | `↑` `↓` / `Tab` | Chọn mục |
| Cài đặt | `Enter` | Sửa mục (với "Tự cào proxy": bật/tắt) |
| Cài đặt | `S` / `F5` | Bắt đầu |
| Chạy | `↑` `↓` | Cuộn bảng luồng |
| Chạy | `Esc` | Dừng, về cài đặt |
| Cả hai | `Q` / `Ctrl+C` | Thoát |

## Cài đặt (`settings.txt`)

File dạng `key=value`, được tạo tự động, có thể sửa tay.

| Key | Ý nghĩa | Mặc định |
|---|---|---|
| `url` | Link cần gửi request | trống |
| `streams` | Số luồng | 5 |
| `interval` | Chu kỳ gửi (giây) | 600 |
| `scrape` | `yes`/`no`: tự cào proxy | yes |
| `proxy_file` | File proxy thủ công, mỗi dòng 1 proxy, `#` là chú thích | trống |
| `timeout` | Timeout kiểm tra proxy (giây) | 8 |
| `max_check` | Số proxy kiểm tra tối đa | 1000 |
| `sources` | Danh sách nguồn proxy, cách nhau bằng dấu phẩy | 3 nguồn công khai |

Nếu `scrape=no` và không có `proxy_file`, các luồng đi thẳng, không qua proxy.

## Hạn chế đã biết

- Proxy miễn phí chết rất nhiều và đổi liên tục. Chương trình **không tự thay proxy** khi nó chết giữa chừng; hãy dừng và chạy lại để cào mới.
- Chu kỳ 10 phút thường không giữ được socket mở vì server hay đóng kết nối nhàn rỗi sớm hơn; mỗi lần gửi có thể phải mở kết nối mới.
- Bước kiểm tra proxy cũng gửi request đến link đích.
- Proxy công khai có thể đọc/chỉnh lưu lượng. Đừng gửi dữ liệu nhạy cảm qua đó.
- Bảng luồng chưa có cuộn chuột, chỉ cuộn bằng phím.
- Chưa có test tự động.

## Cấu trúc

```
src/main.rs   toàn bộ mã nguồn (cài đặt, cào/kiểm tra proxy, worker, TUI)
```
