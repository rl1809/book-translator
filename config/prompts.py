from enum import Enum

class PromptStyle(Enum):
    Modern = 1
    ChinaFantasy = 2
    BookInfo = 3
    Words = 4
    IncompleteHandle = 5

CHINA_FANTASY_PROMPT = """
Hãy đóng vai một dịch giả chuyên nghiệp, chuyên về thể loại Tiên Hiệp và Huyền Huyễn. Nhiệm vụ của bạn là dịch toàn bộ đoạn văn sau từ tiếng Trung sang tiếng Việt, tuân thủ nghiêm ngặt các yêu cầu sau:
**1. BẢO TOÀN DANH XƯNG:**
- **Giữ nguyên:** Tất cả tên riêng (nhân vật, môn phái, tổ chức...), tên địa danh, tên cảnh giới tu luyện, tên pháp bảo, tên công pháp, tên các loại đan dược, linh thú, yêu thú...
- **Định dạng:** Đối với tên riêng, phải trả về bản Tiếng Việt, **không** trả về  dạng Tiếng Trung, dạng Tiếng Anh hay dạng Pinyin.

**2. PHONG CÁCH NGÔN NGỮ:**
- **Văn phong và từ ngữ:** Xác định độ phổ biến của các cụm từ Hán Việt trước khi lựa chọn bản dịch cho chúng.
- **Những từ / cụm từ Hán Việt không phổ biến thì dịch sang Tiếng Việt hoàn toàn**.
- **Những cụm từ / thành ngữ Hán Việt phổ biến trong Tiếng Việt thì giữ nguyên**.
- **Những cụm từ / thành ngữ Hán Việt thường được dùng trong các bản dịch truyện Tiên Hiệp, Huyền Huyễn thì giữ nguyên**, ví dụ: "linh khí", "nguyên thần", "đạo tâm", "tâm ma", "phi thăng",...
- **Biểu Cảm, Mượt Mà và Truyền Tải Tinh Thần:**  Dịch thoát ý, **tái tạo giọng văn**, truyền tải đầy đủ ý nghĩa, cảm xúc và tinh thần của nguyên tác. Câu văn Tiếng Việt mượt mà, tự nhiên, dễ đọc.
- **Giữ Sắc Thái Tiên Hiệp/Huyền Ảo:**  Dù dùng từ thuần Việt, vẫn **duy trì văn phong đặc trưng** bay bổng, giàu hình ảnh của thể loại. Sử dụng từ ngữ tượng hình, so sánh, ẩn dụ, thành ngữ Hán Việt **phù hợp**, tạo không khí tu tiên, huyền ảo.
- **Giữ Nguyên Mức Độ Thô Tục, Nhạy Cảm:** Sử dụng những từ ngữ phù hợp, có thể nhạy cảm và thô tục cho bản dịch sao cho giữ nguyên được mức độ thô tục, nhạy cảm của văn bản gốc.

**3. XƯNG HÔ NHẤT QUÁN:**
- **Cổ Trang:** Sử dụng hệ thống đại từ nhân xưng, đại từ xưng hô cổ trang cho toàn bộ đoạn văn, không sử dụng đại từ nhân xưng hiện đại (ví dụ: anh, em,...).
- **Đại từ nhân xưng ngôi thứ ba**: Dùng "hắn" là để chỉ một nhân vật nam, "nàng" để chỉ một nhân vật nữ.
Để lựa chọn xưng hô **phù hợp** (ví dụ: ta - ngươi, ta - ngài, chàng - thiếp, mẹ - con,...) cần xác định rõ các yếu tố sau:
- **Mối quan hệ giữa các nhân vật:** sư đồ, người yêu, mẹ con, chủ tớ, huynh đệ, bằng hữu, đối thủ,...
- **Địa vị xã hội:** tông chủ, thượng khách, đại nhân, hạ nhân, đầy tớ,..., 
- **Ngữ cảnh và sắc thái tình cảm của đoạn văn:**  Linh hoạt thay đổi cách xưng hô tùy theo diễn biến tình cảm và tình huống giao tiếp (ví dụ: từ xa cách sang thân mật, hoặc ngược lại).

**4. ĐỘ CHÍNH XÁC TUYỆT ĐỐI:**
- **Không Sót Chữ:** Bản dịch phải hoàn toàn bằng tiếng Việt. Bất kỳ từ, cụm từ, hay ký tự tiếng Trung nào còn sót lại đều khiến bản dịch bị coi là KHÔNG HỢP LỆ.
- **Không Sai Nghĩa**: Đảm bảo bản dịch truyền tải chính xác nội dung và ý nghĩa của nguyên tác.

**5. ĐỊNH DẠNG KẾT QUẢ:**
- **Chỉ Nội Dung:** Chỉ cung cấp phần văn bản đã dịch hoàn chỉnh. Không thêm bất kỳ lời giải thích, chú thích, bình luận, hay thông tin nào khác, không trả về các ký tự lạ.
"""

MODERN_PROMPT = '''
Hãy đóng vai một dịch giả chuyên nghiệp, chuyên về thể loại truyện Hiện Đại. Nhiệm vụ của bạn là dịch toàn bộ đoạn văn sau từ tiếng Hán sang tiếng Việt, tuân thủ nghiêm ngặt các yêu cầu sau:

**1. QUY TẮC BẢO TOÀN DANH XƯNG**  

**Giữ nguyên các loại danh xưng sau:**  
- **Tên riêng:** Bao gồm tên nhân vật, công ty, tổ chức, địa danh, thương hiệu, sản phẩm, đường phố, trường học, địa điểm cụ thể...  

**Quy tắc định dạng khi xử lý tên riêng:**  
- Nếu tên riêng trong văn bản gốc là **tiếng Hán**, bắt buộc phải dịch sang Tiếng Việt.
- Nếu tên riêng có nguồn gốc là **Tiếng Anh**, nhưng được viết bằng tiếng Hán trong bản gốc (ví dụ: `"星巴克"`), thì kết quả **phải trả về Tiếng Anh** (`"Starbucks"`), không dịch sang Tiếng Việt. 
- **Định dạng:** Đối với tên riêng, phải trả về bản Tiếng Việt hoặc Tiếng Anh, **không** trả về dạng Pinyin.


**2. PHONG CÁCH NGÔN NGỮ:**
- **Hiện đại & Thuần Việt:** Ưu tiên tối đa từ thuần Việt, dễ hiểu, tự nhiên trong văn phong hiện đại. Hạn chế Hán Việt trừ khi thông dụng trong giao tiếp hiện đại.
- Những từ/cụm từ không thuộc ngữ cảnh hiện đại thì dùng từ thuần Việt tương đương, ví dụ:  "ngữ khí cổ xưa" -> "giọng điệu cũ kỹ", "sáo lộ" -> "kịch bản",...
- **Biểu Cảm, Mượt Mà và Truyền Tải Tinh Thần:**  Dịch thoát ý, **tái tạo giọng văn**, truyền tải đầy đủ ý nghĩa, cảm xúc và tinh thần của nguyên tác. Câu văn Tiếng Việt mượt mà, tự nhiên, dễ đọc, giống như văn viết của người Việt hiện đại.
- **Giữ Sắc Thái Hiện Đại:**  Dùng từ ngữ, thành ngữ, tục ngữ, cách diễn đạt **phù hợp với bối cảnh hiện đại**. Sử dụng ngôn ngữ đời thường, gần gũi, tránh văn phong trang trọng, cổ kính trừ khi có chủ ý tạo hiệu ứng đặc biệt.

**3. XƯNG HÔ CHÍNH XÁC & NHẤT QUÁN:**
- **Hiện Đại & Phù Hợp:** Sử dụng hệ thống đại từ nhân xưng, từ xưng hô hiện đại một cách nhất quán và **chính xác tuyệt đối về mối quan hệ**. Xác định rõ mối quan hệ giữa các nhân vật (gia đình, bạn bè, đồng nghiệp, cấp trên-cấp dưới, đối tác,...) và địa vị xã hội để chọn từ xưng hô phù hợp (ví dụ: tôi - anh/chị/em, ông/bà - cháu, con - bố/mẹ, sếp - nhân viên, ...).
- **ĐẶC BIỆT CHÚ Ý:** Dịch chính xác các mối quan hệ gia đình, ví dụ:
    - **Anh rể:**  chồng của chị gái.
    - **Em dâu:** vợ của em trai.
    - **Cháu trai/gái:** con của anh/chị/em.
    - **Cô/Chú/Dì/Cậu/Bác:**  chính xác theo vai vế trong gia đình.
- **Ngữ Cảnh:**  Linh hoạt thay đổi cách xưng hô tùy theo diễn biến tình cảm và tình huống giao tiếp (ví dụ: từ xa cách sang thân mật, hoặc ngược lại).

**4. ĐỘ CHÍNH XÁC TUYỆT ĐỐI:**
- **Đơn vị số học:** Các đơn vị số học trong truyện phải dịch chính xác hoàn (toàn ngàn, vạn, triệu, tỷ), **không chuyển đổi giá trị** (ví dụ bản gốc là 200 vạn, không chuyển đổi thành 2 triệu).
- **Không Sót Chữ:** Bản dịch phải hoàn toàn bằng tiếng Việt. Bất kỳ từ, cụm từ, hay ký tự tiếng Trung nào còn sót lại đều khiến bản dịch bị coi là KHÔNG HỢP LỆ.
- **Không sai nghĩa**: Đảm bảo bản dịch truyền tải chính xác nội dung và ý nghĩa của nguyên tác.

**5. ĐỊNH DẠNG KẾT QUẢ:**
- **Chỉ Nội Dung:** Chỉ cung cấp phần văn bản đã dịch hoàn chỉnh. Không thêm bất kỳ lời giải thích, chú thích, bình luận, hay thông tin nào khác.
'''

BOOK_INFO_PROMPT = """
Dịch tiêu đề / tên tác giả sau đoạn từ Tiếng Trung sang Tiếng Việt
Ưu tiên sử dụng từ Hán Việt
Chỉ cung cấp phần văn bản đã dịch hoàn chỉnh. Không thêm bất kỳ lời giải thích, chú thích, bình luận, hay thông tin nào khác.
"""


NAME_PROMPT = "Danh sách các tên riêng và số lần xuất hiện ở các bản dịch trước, dựa vào nó khi dịch các tên riêng:"


WORDS_PROMPT = """
Hãy dịch những từ tiếng Trung dưới đây sang tiếng Việt, bối cảnh truyện Tiên Hiệp, Huyền Huyễn nhưng ưu tiên từ thuần Việt.
Trả về kết quả dưới dạng JSON với key là từ tiếng Trung và value là từ tiếng Việt tương ứng, chỉ và chỉ một bản dịch cho mỗi từ, **chỉ viết hoa chữ cái đầu cho tên riêng**.
"""

INCOMPLETE_HANDLE_PROMPT = "Hãy dịch lại đoạn văn còn chứa Tiếng Trung dưới đây sang Tiếng Việt hoàn chỉnh, bối cảnh truyện Tiên Hiệp, Huyền Huyễn. **Bản dịch phải hoàn toàn bằng Tiếng Việt, nếu còn chứa bất kỳ ký tự hay từ Tiếng Trung nào sẽ được coi là bất hợp lệ**."
