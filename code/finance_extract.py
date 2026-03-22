# ===================== 1. 导入依赖库 =====================
import pdfplumber
import pandas as pd
import sqlite3
import re
import os
from pathlib import Path

# ===================== 2. 配置文件路径（仅需修改这里！） =====================
# 你的仓库根目录（不用改，自动识别）
ROOT_DIR = Path("D:/GitRepos/TaiDiBei")
# 财报PDF文件夹路径
PDF_DIR = ROOT_DIR / "data" / "pdf"
# 附件3Excel路径（替换为你的附件3文件名）
ATTACH3_EXCEL = ROOT_DIR / "data" / "excel" / "附件3：数据库-表名及字段说明.xlsx"
# 输出数据库文件路径
DB_PATH = ROOT_DIR / "output" / "finance_database.db"

# ===================== 3. 字段别名映射表（核心，已适配附件3） =====================
# 核心业绩指标表映射
core_perf_mapping = {
    "stock_code": {"cn_name": "股票代码", "aliases": ["股票代码", "证券代码", "代码"], "type": "varchar(20)"},
    "stock_abbr": {"cn_name": "股票简称", "aliases": ["股票简称", "证券简称", "公司简称", "简称"],
                   "type": "varchar(50)"},
    "eps": {"cn_name": "每股收益(元)", "aliases": ["每股收益", "基本每股收益", "EPS"], "type": "decimal(10,4)"},
    "total_operating_revenue": {"cn_name": "营业总收入(万元)", "aliases": ["营业总收入", "营业收入", "营收总额"],
                                "type": "decimal(20,2)"},
    "net_profit_10k_yuan": {"cn_name": "净利润(万元)", "aliases": ["净利润", "归母净利润"], "type": "decimal(20,2)"},
    "report_period": {"cn_name": "报告期", "aliases": ["报告期", "会计期间"], "type": "varchar(20)"},
    "report_year": {"cn_name": "报告期-年份", "aliases": ["报告年份", "会计年度"], "type": "int"}
}

# 资产负债表核心映射（简化版，可根据附件3补充完整）
balance_sheet_mapping = {
    "stock_code": {"cn_name": "股票代码", "aliases": ["股票代码"], "type": "varchar(20)"},
    "asset_total_assets": {"cn_name": "资产-总资产(万元)", "aliases": ["总资产", "资产总计"], "type": "decimal(20,2)"},
    "liability_total_liabilities": {"cn_name": "负债-总负债(万元)", "aliases": ["总负债", "负债总计"],
                                    "type": "decimal(20,2)"},
    "report_period": {"cn_name": "报告期", "aliases": ["报告期"], "type": "varchar(20)"},
    "report_year": {"cn_name": "报告期-年份", "aliases": ["报告年份"], "type": "int"}
}


# ===================== 4. 核心函数：提取PDF财报内容 =====================
def extract_finance_report(pdf_path):
    """提取PDF财报的文本和表格"""
    report_content = {"core_perf_text": "", "balance_sheet": None}
    try:
        with pdfplumber.open(pdf_path) as pdf:
            # 提取全文本
            full_text = ""
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    full_text += page_text + "\n"

            # 定位核心业绩指标
            if "主要会计数据和财务指标" in full_text:
                start = full_text.find("主要会计数据和财务指标")
                end = full_text.find("合并资产负债表") if "合并资产负债表" in full_text else len(full_text)
                report_content["core_perf_text"] = full_text[start:end]

            # 提取资产负债表表格
            for page in pdf.pages:
                page_text = page.extract_text()
                if "合并资产负债表" in page_text and "单位：元" in page_text:
                    tables = page.extract_tables()
                    if tables:
                        df = pd.DataFrame(tables[0])
                        df.columns = df.iloc[0]
                        df = df[1:].reset_index(drop=True)
                        report_content["balance_sheet"] = df
        return report_content
    except Exception as e:
        print(f"提取{pdf_path}失败：{e}")
        return report_content


# ===================== 5. 核心函数：从文本/表格提取数据 =====================
def extract_from_text(text, mapping_dict):
    """从纯文本提取数据"""
    result = {}
    text = text.replace("\n", " ").replace(" ", "")
    for std_field, field_info in mapping_dict.items():
        aliases = [alias.strip() for alias in field_info["aliases"]]
        alias_pattern = "|".join([re.escape(alias) for alias in aliases])
        pattern = rf"({alias_pattern})[:：]?([-+]?[\d,]+(\.\d+)?)[万元%元]?"
        match = re.search(pattern, text)
        if match:
            value_str = match.group(2).replace(",", "")
            try:
                value = float(value_str)
                if "元" in match.group(0) and "万元" not in match.group(0):
                    value /= 10000  # 元转万元
                result[std_field] = value
            except:
                result[std_field] = None
    return result


def extract_from_table(df, mapping_dict):
    """从表格提取数据"""
    result = {}
    if df is None:
        return result
    df.iloc[:, 0] = df.iloc[:, 0].astype(str).str.strip()
    for std_field, field_info in mapping_dict.items():
        aliases = [alias.strip() for alias in field_info["aliases"]]
        for _, row in df.iterrows():
            row_name = row.iloc[0]
            if any(alias in row_name for alias in aliases):
                value = row.iloc[1]
                if isinstance(value, str):
                    value = value.replace(",", "").replace("元", "")
                    try:
                        value = float(value) / 10000  # 元转万元
                    except:
                        value = None
                result[std_field] = value
                break
    return result


# ===================== 6. 核心函数：创建数据库+写入数据 =====================
def init_database():
    """初始化数据库表（适配附件3）"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # 创建核心业绩表
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS core_performance_indicators_sheet (
        serial_number INTEGER PRIMARY KEY AUTOINCREMENT,
        stock_code VARCHAR(20),
        stock_abbr VARCHAR(50),
        eps DECIMAL(10,4),
        total_operating_revenue DECIMAL(20,2),
        net_profit_10k_yuan DECIMAL(20,2),
        report_period VARCHAR(20),
        report_year INT
    )
    """)
    # 创建资产负债表
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS balance_sheet (
        serial_number INTEGER PRIMARY KEY AUTOINCREMENT,
        stock_code VARCHAR(20),
        asset_total_assets DECIMAL(20,2),
        liability_total_liabilities DECIMAL(20,2),
        report_period VARCHAR(20),
        report_year INT
    )
    """)
    conn.commit()
    conn.close()


def write_to_db(cleaned_data, table_name):
    """写入数据库"""
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.DataFrame([cleaned_data])
        df.to_sql(table_name, conn, if_exists="append", index=False)
        conn.commit()
        print(f"✅ 成功写入{table_name}：{cleaned_data.get('stock_code', '未知')}")
    except Exception as e:
        print(f"❌ 写入失败：{e}")
    finally:
        conn.close()


# ===================== 7. 主流程：单文件测试 =====================
if __name__ == "__main__":
    # 1. 初始化数据库
    init_database()

    # 2. 选择一个测试PDF（替换为你的财报PDF文件名）
    test_pdf = PDF_DIR / "平安银行2023年报.pdf"  # 改成你实际的PDF文件名
    if not test_pdf.exists():
        print(f"❌ 找不到PDF文件：{test_pdf}，请检查路径！")
    else:
        # 3. 提取财报内容
        report_data = extract_finance_report(test_pdf)

        # 4. 手动补充股票基础信息（从PDF文件名/财报里提取）
        stock_info = {
            "stock_code": "000001",  # 替换为实际股票代码
            "stock_abbr": "平安银行",  # 替换为实际股票简称
            "report_period": "FY",  # 年报=FY/一季报=Q1
            "report_year": 2023  # 替换为实际年份
        }

        # 5. 提取核心业绩数据
        core_perf_data = extract_from_text(report_data["core_perf_text"], core_perf_mapping)
        core_perf_data.update(stock_info)  # 补充基础信息

        # 6. 提取资产负债表数据
        balance_data = extract_from_table(report_data["balance_sheet"], balance_sheet_mapping)
        balance_data.update(stock_info)

        # 7. 写入数据库
        write_to_db(core_perf_data, "core_performance_indicators_sheet")
        write_to_db(balance_data, "balance_sheet")

        print("\n🎉 单文件测试完成！可在output/finance_database.db查看结果")