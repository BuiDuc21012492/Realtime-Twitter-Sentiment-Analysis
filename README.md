## Mục tiêu tổng quát:
-	Xây dựng thành công một pipeline dữ liệu (data pipeline) hoàn chỉnh, tự động, từ khâu nhập dữ liệu thô đến khâu hiển thị kết quả phân tích cảm xúc một cách trực quan.

 ## Đối tượng và phạm vi nghiên cứu
### 1. Đối tượng nghiên cứu: Đối tượng chính của dự án là các công nghệ và khái niệm nền tảng trong lĩnh vực kỹ thuật dữ liệu và dữ liệu lớn, bao gồm:
-	Kiến trúc hệ thống hướng dịch vụ (Microservices).
-	Công nghệ container hóa với Docker và Docker Compose.
-	Hệ thống hàng đợi tin nhắn phân tán Apache Kafka và thành phần phụ trợ Zookeeper.
-	Ngôn ngữ lập trình Python và các thư viện chuyên dụng: kafka-python để tương tác với Kafka, pandas để xử lý dữ liệu, VaderSentiment để phân tích cảm xúc, và Dash by Plotly để xây dựng dashboard tương tác.
### 2. Phạm vi nghiên cứu: Dự án tập trung vào việc xây dựng và triển khai một hệ thống hoàn chỉnh với các giới hạn cụ thể sau:
- Dữ liệu đầu vào: Sử dụng một tập dữ liệu tĩnh có sẵn là file Tweets.csv, chứa 14,640 bình luận bằng tiếng Anh liên quan đến hãng hàng không American Airlines.
-	Mô hình phân tích: Việc phân tích cảm xúc được thực hiện ở mức độ cơ bản (phân loại thành tích cực, tiêu cực, trung tính) dựa trên từ điển của thư viện VaderSentiment.
-	Môi trường triển khai: Toàn bộ hệ thống được thiết kế để triển khai trên một máy tính cá nhân (localhost) thông qua Docker Compose. Dự án không đi sâu vào việc triển khai trên một cụm máy chủ (cluster) thực tế.
## Mô tả luồng dữ liệu
Luồng dữ liệu của hệ thống hoạt động theo 5 bước chính:
1.	Bước 1 - Gửi dữ liệu (Produce): Dịch vụ Producer khởi động, đọc dữ liệu từ file Tweets.csv. Mỗi dòng trong file được xử lý, định dạng thành một message JSON và gửi vào topic amazon_reviews trên Kafka. Sau khi gửi hết dữ liệu, dịch vụ này tự động dừng lại.
2.	Bước 2 - Xử lý cảm xúc (Consume & Process): Dịch vụ Consumer (Sentiment Processor) liên tục lắng nghe topic amazon_reviews. Khi có message mới, nó sẽ tiêu thụ message đó, trích xuất nội dung văn bản (tweet) và sử dụng thư viện VaderSentiment để phân tích và gán nhãn cảm xúc (positive, negative, neutral).
3.	Bước 3 - Gửi kết quả (Re-produce): Sau khi phân tích, dịch vụ Consumer tạo một message JSON mới chứa dữ liệu đã được làm giàu (bao gồm văn bản gốc, nhãn cảm xúc, và timestamp). Message này sau đó được gửi vào một topic thứ hai là sentiment_topic.
4.	Bước 4 - Thu thập kết quả (Consume for Dashboard): Dịch vụ Dashboard hoạt động như một consumer, liên tục lắng nghe topic sentiment_topic để nhận các kết quả phân tích mới nhất. Dữ liệu này được lưu trữ tạm thời trong bộ nhớ của dịch vụ.
5.	Bước 5 - Trực quan hóa (Visualize): Giao diện web của Dashboard, được xây dựng bằng Dash, tự động làm mới sau mỗi 2 giây. Trong mỗi lần làm mới, nó đọc dữ liệu đã thu thập được và vẽ lại biểu đồ tròn, cung cấp cho người dùng một cái nhìn cập nhật về phân phối cảm xúc của toàn bộ tập dữ liệu.

## Cấu truc thư mục
