
from utils import TableCreator
class MetaSurrogateRepository:
    """Lưu surrogate key đã generate vào meta_surrogate_map"""

    def __init__(self, postgres_client):
        self.pg = postgres_client

    def get_or_create(self, natural_key: str, type_name: str, generator: TableCreator):
        sql_get = """
            SELECT surrogate_key FROM meta_surrogate_map
            WHERE natural_key=%s AND type=%s AND valid_to IS NULL;
        """
        res = self.pg.fetch_one(sql_get, (natural_key, type_name))

        if res:
            return res[0]

        # generate mới
        surrogate_key = generator.get_id()
        sql_ins = """
            INSERT INTO meta_surrogate_map(natural_key, surrogate_key, type, valid_from)
            VALUES(%s,%s,%s, NOW())
        """
        self.pg.execute(sql_ins, (natural_key, surrogate_key, type_name))
        return surrogate_key


dim_market_type_info = {
    "table_name": "dim_market_type",
    "primary_key": "market_key",
    "market_key_char": "CP68MARKETKEY",
    "market_name": {"HOSE": "Sở giao dịch chứng khoán Hồ Chí Minh",
                    "HNX": "Sở giao dịch chứng khoán Hà Nội",
                    "UPCOM": "Sở giao dịch có các công ty chưa niêm yết",
                    "VN30": "Rổ chứng khoán gồm 30 cổ phiếu lớn nhất và có tính thanh khoản cao nhất toàn sàn"},
    
}



dim_industry_info = {
    "table_name": "dim_industry",
    "primary_key": "industry_key",
    "market_key_char": "CP68INDUSTRYINFOKEY",
    "industry_mapping" : {
        "Bán buôn": "^bb",
        "Bất động sản": "^bds",
        "Bảo hiểm": "^bh",
        "Bán lẻ": "^bl",
        "Chế biến Thủy sản": "^cbts",
        "Chứng khoán": "^ck",
        "Công nghệ và Thông tin": "^cntt",
        "Chăm sóc sức khỏe": "^cssk",
        "Dịch vụ lưu trú, ăn uống, giải trí": "^dvltaugt",
        "Dịch vụ tư vấn, hỗ trợ": "^dvtvht",
        "Khai khoáng": "^kk",
        "Ngân hàng": "^nh",
        "Nông - Lâm - Ngư nghiệp": "^nln",
        "Sản phẩm cao su": "^spcs",
        "Sản xuất Hàng gia dụng": "^sxhgd",
        "Sản xuất Nhựa - Hóa chất": "^sxnhc",
        "Sản xuất Phụ trợ": "^sxpt",
        "Sản xuất Thiết bị, máy móc": "^sxtbmm",
        "Thiết bị điện": "^tbd",
        "Tài chính khác": "^tck",
        "Tiện ích": "^ti",
        "Thực phẩm - Đồ uống": "^tpdu",
        "Vật liệu xây dựng": "^vlxd",
        "Vận tải - kho bãi": "^vtkb",
        "Xây dựng": "^xd",
        "Cao su": "^caosu",
        "Nhóm Dầu khí": "^daukhi",
        "Dược phẩm / Y tế / Hóa chất": "^duocpham",
        "Giáo dục": "^giaoduc",
        "Hàng không": "^hk",
        "Năng lượng (Điện/Khí/...)": "^nangluong",
        "Nhựa - Bao bì": "^nhua",
        "Phân bón": "^phanbon",
        "Ngành Thép": "^thep",
    }
}