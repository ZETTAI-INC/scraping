"""
ハローワーク求人スクレイパー
https://www.hellowork.mhlw.go.jp/kensaku/GECA110010.do

職業分類コード（大分類）で検索する方式
"""

import asyncio
import re
import logging
from typing import Dict, List, Any, Optional
from playwright.async_api import Page, TimeoutError as PlaywrightTimeout
from .base_scraper import BaseScraper


# 都道府県コード (01-47)
PREFECTURE_CODES: Dict[str, str] = {
    "北海道": "01",
    "青森県": "02",
    "岩手県": "03",
    "宮城県": "04",
    "秋田県": "05",
    "山形県": "06",
    "福島県": "07",
    "茨城県": "08",
    "栃木県": "09",
    "群馬県": "10",
    "埼玉県": "11",
    "千葉県": "12",
    "東京都": "13",
    "神奈川県": "14",
    "新潟県": "15",
    "富山県": "16",
    "石川県": "17",
    "福井県": "18",
    "山梨県": "19",
    "長野県": "20",
    "岐阜県": "21",
    "静岡県": "22",
    "愛知県": "23",
    "三重県": "24",
    "滋賀県": "25",
    "京都府": "26",
    "大阪府": "27",
    "兵庫県": "28",
    "奈良県": "29",
    "和歌山県": "30",
    "鳥取県": "31",
    "島根県": "32",
    "岡山県": "33",
    "広島県": "34",
    "山口県": "35",
    "徳島県": "36",
    "香川県": "37",
    "愛媛県": "38",
    "高知県": "39",
    "福岡県": "40",
    "佐賀県": "41",
    "長崎県": "42",
    "熊本県": "43",
    "大分県": "44",
    "宮崎県": "45",
    "鹿児島県": "46",
    "沖縄県": "47",
}

# 職業分類コード（大分類）
# 厚生労働省編職業分類に基づく大分類コード（2桁）
# 参照: https://www.hellowork.mhlw.go.jp/info/mhlw_job_dictionary.html
JOB_CATEGORY_CODES: Dict[str, str] = {
    "管理的職業": "01",
    "研究・技術の職業": "02",
    "法務・経営・文化芸術等の専門的職業": "03",
    "医療・看護・保健の職業": "04",
    "保育・教育の職業": "05",
    "事務的職業": "06",
    "販売・営業の職業": "07",
    "福祉・介護の職業": "08",
    "サービスの職業": "09",
    "警備・保安の職業": "10",
    "農林漁業の職業": "11",
    "製造・修理・塗装・製図等の職業": "12",
    "配送・輸送・機械運転の職業": "13",
    "建設・土木・電気工事の職業": "14",
    "運搬・清掃・包装・選別等の職業": "15",
}

# キーワードから職業分類コードへのマッピング
# 厚生労働省編職業分類に基づく（中分類コードを使用）
# 参照: https://www.hellowork.mhlw.go.jp/info/mhlw_job_dictionary.html
#
# コード体系:
#   大分類: 2桁 (01-15)
#   中分類: 3桁 (001-099) ← 検索で使用するコード
#   小分類: 3桁-2桁 (001-01 など)
#
# ハローワーク検索フォームでは中分類コード（3桁）を入力
KEYWORD_TO_CATEGORY: Dict[str, str] = {
    # ========================================
    # 01: 管理的職業
    # 中分類: 001法人・団体役員, 002法人・団体管理職員, 003その他の管理的職業
    # ========================================
    "管理": "002", "マネージャー": "002", "役員": "001", "経営者": "001",
    "部長": "002", "課長": "002", "支配人": "002", "管理職": "002",
    "会社役員": "001",

    # ========================================
    # 02: 研究・技術の職業
    # 中分類: 004研究者, 005農林水産技術者, 006開発技術者, 007製造技術者,
    #         008建築・土木・測量技術者, 009情報処理・通信技術者(ソフトウェア開発),
    #         010情報処理・通信技術者(ソフトウェア開発を除く), 011その他の技術の職業
    # ========================================
    # 004 研究者
    "研究者": "004", "研究": "004", "研究員": "004",
    # 005 農林水産技術者
    "農林水産技術者": "005",
    # 006 開発技術者
    "開発技術者": "006", "食品開発": "006", "電気開発": "006", "電子開発": "006",
    "機械開発": "006", "自動車開発": "006", "化学開発": "006", "材料開発": "006",
    # 007 製造技術者
    "製造技術者": "007", "電気工事技術者": "007", "生産技術": "007",
    # 008 建築・土木・測量技術者
    "建築設計": "008", "建築士": "008", "施工管理": "008", "土木設計": "008",
    "測量": "008", "測量士": "008", "建築技術者": "008", "土木技術者": "008",
    # 009 情報処理・通信技術者（ソフトウェア開発）
    "IT": "009", "SE": "009", "システムエンジニア": "009", "プログラマ": "009",
    "プログラマー": "009", "ソフトウェア開発": "009", "WEB開発": "009",
    "組込": "009", "制御系": "009", "エンジニア": "009",
    # 010 情報処理・通信技術者（ソフトウェア開発を除く）
    "ITコンサルタント": "010", "システム設計": "010", "プロジェクトマネージャ": "010",
    "システム運用": "010", "ITヘルプデスク": "010", "ネットワーク": "010",
    "ネットワークエンジニア": "010", "インフラエンジニア": "010",
    # 011 その他の技術の職業
    "通信機器操作": "011", "技術": "011", "開発": "006", "設計": "008",

    # ========================================
    # 03: 法務・経営・文化芸術等の専門的職業
    # 中分類: 012法務の職業, 013経営・金融・保険の専門的職業, 014宗教家,
    #         015著述家・記者・編集者, 016美術家・写真家・映像撮影者, 017デザイナー,
    #         018音楽家・舞台芸術家, 019図書館司書・学芸員・カウンセラー,
    #         020その他の法務・経営・文化芸術等の専門的職業
    # ========================================
    # 012 法務の職業
    "法務": "012", "弁護士": "012", "弁理士": "012", "司法書士": "012",
    # 013 経営・金融・保険の専門的職業
    "公認会計士": "013", "税理士": "013", "社会保険労務士": "013", "社労士": "013",
    "経営コンサルタント": "013", "金融専門": "013",
    # 014 宗教家
    "宗教家": "014",
    # 015 著述家、記者、編集者
    "著述家": "015", "ライター": "015", "翻訳": "015", "翻訳家": "015",
    "記者": "015", "編集": "015", "編集者": "015",
    # 016 美術家、写真家、映像撮影者
    "美術家": "016", "イラストレーター": "016", "写真家": "016", "カメラマン": "016",
    "映像撮影": "016",
    # 017 デザイナー
    "デザイナー": "017", "ウェブデザイナー": "017", "WEBデザイナー": "017",
    "グラフィックデザイナー": "017", "デザイン": "017",
    # 018 音楽家、舞台芸術家
    "音楽家": "018", "舞踊家": "018", "俳優": "018", "演出家": "018", "プロデューサー": "018",
    # 019 図書館司書、学芸員、カウンセラー
    "図書館司書": "019", "司書": "019", "学芸員": "019", "カウンセラー": "019",
    # 020 その他
    "通訳": "020", "スポーツ選手": "020", "アナウンサー": "020",

    # ========================================
    # 04: 医療・看護・保健の職業
    # 中分類: 021医師・歯科医師・獣医師・薬剤師, 022保健師・助産師,
    #         023看護師・准看護師, 024医療技術者, 025栄養士・管理栄養士,
    #         026あん摩マッサージ指圧師・はり師・きゅう師・柔道整復師,
    #         027その他の医療・看護・保健の専門的職業, 028保健医療関係助手
    # ========================================
    # 021 医師、歯科医師、獣医師、薬剤師
    "医師": "021", "歯科医師": "021", "獣医師": "021", "薬剤師": "021",
    # 022 保健師、助産師
    "保健師": "022", "助産師": "022",
    # 023 看護師、准看護師
    "看護師": "023", "准看護師": "023", "看護": "023",
    # 024 医療技術者
    "診療放射線技師": "024", "放射線技師": "024", "臨床工学技士": "024",
    "臨床検査技師": "024", "理学療法士": "024", "作業療法士": "024",
    "視能訓練士": "024", "言語聴覚士": "024", "歯科衛生士": "024", "歯科技工士": "024",
    # 025 栄養士、管理栄養士
    "栄養士": "025", "管理栄養士": "025",
    # 026 あん摩マッサージ指圧師、はり師、きゅう師、柔道整復師
    "あん摩": "026", "マッサージ": "026", "はり師": "026", "きゅう師": "026",
    "鍼灸師": "026", "柔道整復師": "026", "整体": "026",
    # 028 保健医療関係助手
    "看護助手": "028", "歯科助手": "028", "医療": "028",

    # ========================================
    # 05: 保育・教育の職業
    # 中分類: 029保育士・幼稚園教員, 030学童保育等指導員・保育補助者・家庭的保育者,
    #         031学校等教員, 032習い事指導等教育関連の職業
    # ========================================
    # 029 保育士、幼稚園教員
    "保育士": "029", "幼稚園教員": "029", "保育教諭": "029", "保育": "029",
    # 030 学童保育等指導員、保育補助者、家庭的保育者
    "学童保育": "030", "学童": "030", "児童館": "030", "保育補助": "030",
    # 031 学校等教員
    "教員": "031", "教師": "031", "小学校教員": "031", "中学校教員": "031",
    "高校教員": "031", "大学教員": "031", "特別支援学校": "031",
    # 032 習い事指導等教育関連の職業
    "講師": "032", "塾講師": "032", "塾": "032", "インストラクター": "032",
    "スポーツ指導": "032", "教育": "032", "幼稚園": "029",

    # ========================================
    # 06: 事務的職業
    # 中分類: 033総務・人事・企画事務, 034一般事務・秘書・受付,
    #         035その他の総務等事務, 036電話・インターネットによる応接事務,
    #         037医療・介護事務, 038会計事務, 039生産関連事務,
    #         040営業・販売関連事務, 041外勤事務, 042運輸・郵便事務,
    #         043コンピュータ等事務用機器操作
    # ========================================
    # 033 総務・人事・企画事務
    "総務": "033", "人事": "033", "企画": "033", "総務事務": "033", "人事事務": "033",
    # 034 一般事務・秘書・受付
    "一般事務": "034", "事務": "034", "秘書": "034", "受付": "034", "案内事務": "034",
    # 035 その他の総務等事務
    "法務事務": "035", "広報": "035", "知的財産事務": "035",
    # 036 電話・インターネットによる応接事務
    "コールセンター": "036", "テレフォンアポインター": "036", "テレアポ": "036",
    "電話応対": "036", "カスタマーサポート": "036", "オペレーター": "036",
    # 037 医療・介護事務
    "医療事務": "037", "調剤事務": "037", "介護事務": "037",
    # 038 会計事務
    "経理": "038", "経理事務": "038", "出納": "038", "会計事務": "038",
    # 039 生産関連事務
    "生産事務": "039", "出荷事務": "039",
    # 040 営業・販売関連事務
    "営業事務": "040", "貿易事務": "040",
    # 041 外勤事務
    "集金": "041", "調査員": "041",
    # 042 運輸・郵便事務
    "運輸事務": "042", "郵便事務": "042", "運行管理": "042",
    # 043 コンピュータ等事務用機器操作
    "データ入力": "043", "PC操作": "043", "パソコン操作": "043", "オフィス": "043",

    # ========================================
    # 07: 販売・営業の職業
    # 中分類: 044小売店・卸売店店長, 045販売員, 046商品仕入・再生資源卸売,
    #         047販売類似の職業, 048営業の職業
    # ========================================
    # 044 小売店・卸売店店長
    "店長": "044", "小売店店長": "044", "卸売店店長": "044",
    # 045 販売員
    "販売": "045", "販売員": "045", "レジ": "045", "レジ係": "045",
    "百貨店販売": "045", "コンビニ": "045", "スーパー": "045",
    "食品販売": "045", "衣料品販売": "045", "医薬品販売": "045",
    "化粧品販売": "045", "電気機器販売": "045", "携帯販売": "045",
    "自動車販売": "045", "ガソリンスタンド": "045", "接客販売": "045", "店舗": "045",
    # 046 商品仕入・再生資源卸売
    "仕入": "046", "バイヤー": "046",
    # 047 販売類似の職業
    "不動産仲介": "047", "保険代理": "047",
    # 048 営業の職業
    "営業": "048", "ルート営業": "048", "法人営業": "048", "個人営業": "048",
    "飲食料品営業": "048", "医薬品営業": "048", "MR": "048",
    "機械器具営業": "048", "自動車営業": "048", "通信営業": "048",
    "金融営業": "048", "保険営業": "048", "不動産営業": "048",
    "広告営業": "048", "建設営業": "048", "ショップ": "045",

    # ========================================
    # 08: 福祉・介護の職業
    # 中分類: 049福祉・介護の専門的職業, 050施設介護の職業, 051訪問介護の職業
    # ========================================
    # 049 福祉・介護の専門的職業
    "福祉相談": "049", "福祉指導": "049", "ケアマネジャー": "049", "ケアマネ": "049",
    "介護支援専門員": "049", "サービス提供責任者": "049", "サ責": "049",
    "福祉用具専門相談員": "049", "生活相談員": "049", "相談員": "049",
    # 050 施設介護の職業
    "施設介護": "050", "介護員": "050", "介護職": "050", "介護": "050",
    "老人ホーム": "050", "デイサービス": "050", "グループホーム": "050",
    "介護福祉士": "050", "福祉": "049",
    # 051 訪問介護の職業
    "訪問介護": "051", "ヘルパー": "051", "訪問入浴": "051", "生活支援": "051",
    "障害者支援": "051",

    # ========================================
    # 09: サービスの職業
    # 中分類: 052家庭生活支援サービス, 053理容師・美容師・美容関連サービス,
    #         054浴場・クリーニング, 055飲食物調理, 056接客・給仕,
    #         057居住施設・ビル等の管理, 058その他のサービス
    # ========================================
    # 052 家庭生活支援サービスの職業
    "家政婦": "052", "家事代行": "052",
    # 053 理容師、美容師、美容関連サービスの職業
    "理容師": "053", "美容師": "053", "美容": "053", "理容": "053",
    "エステティシャン": "053", "エステ": "053", "ネイリスト": "053", "ネイル": "053",
    # 054 浴場・クリーニングの職業
    "クリーニング": "054", "洗濯": "054",
    # 055 飲食物調理の職業
    "調理": "055", "調理師": "055", "料理人": "055", "シェフ": "055",
    "日本料理": "055", "西洋料理": "055", "中華料理": "055",
    "給食調理": "055", "惣菜調理": "055", "パティシエ": "055", "パン製造": "055",
    # 056 接客・給仕の職業
    "飲食店店長": "056", "飲食店": "056", "飲食": "056", "ホールスタッフ": "056",
    "ホール": "056", "ウェイター": "056", "ウェイトレス": "056", "接客": "056",
    "バーテンダー": "056", "ソムリエ": "056", "ホテル": "056", "旅館": "056",
    "フロント": "056", "娯楽場": "056",
    # 057 居住施設・ビル等の管理の職業
    "マンション管理人": "057", "寮管理人": "057", "駐車場管理": "057",
    # 058 その他のサービスの職業
    "添乗員": "058", "ツアーコンダクター": "058", "旅行": "058",
    "冠婚葬祭": "058", "葬儀": "058", "ブライダル": "058",
    "ペット": "058", "トリマー": "058", "サービス": "058",

    # ========================================
    # 10: 警備・保安の職業
    # 中分類: 059警備員, 060自衛官, 061司法警察職員, 062看守・消防員,
    #         063その他の保安の職業
    # ========================================
    # 059 警備員
    "警備": "059", "警備員": "059", "施設警備": "059", "交通誘導": "059",
    "ガードマン": "059",
    # 060 自衛官
    "自衛官": "060",
    # 061 司法警察職員
    "警察官": "061",
    # 062 看守、消防員
    "消防": "062", "消防士": "062",
    # 063 その他の保安の職業
    "保安": "063",

    # ========================================
    # 11: 農林漁業の職業
    # 中分類: 064農業の職業, 065林業の職業, 066漁業の職業
    # ========================================
    # 064 農業の職業
    "農業": "064", "稲作": "064", "畑作": "064", "農作物": "064",
    "畜産": "064", "酪農": "064", "家畜": "064", "動物飼育": "064",
    "植木": "064", "造園": "064", "造園師": "064", "園芸": "064",
    # 065 林業の職業
    "林業": "065", "育林": "065", "伐木": "065",
    # 066 漁業の職業
    "漁業": "066", "漁師": "066", "水産養殖": "066",

    # ========================================
    # 12: 製造・修理・塗装・製図等の職業
    # 中分類: 067-070生産設備オペレーター・機械組立設備オペレーター,
    #         071-073製品製造・加工処理工, 074機械組立工, 075機械整備・修理工,
    #         076-079製品検査工・機械検査工, 080生産関連の職業, 081生産類似の職業
    # ========================================
    # 067-070 生産設備オペレーター
    "オペレーター製造": "070", "機械オペレーター": "070", "設備オペレーター": "067",
    # 071-073 製品製造・加工処理工
    "製造": "071", "加工": "071", "工場": "071", "金属加工": "071",
    "食品製造": "072", "プレス": "071", "旋盤": "071", "フライス": "071",
    "NC旋盤": "071", "マシニング": "071", "溶接": "071", "板金": "071",
    "鋳造": "071", "鍛造": "071", "熱処理": "071",
    # 074 機械組立工
    "組立": "074", "機械組立": "074", "電気機器組立": "074", "自動車組立": "074",
    # 075 機械整備・修理工
    "整備": "075", "整備士": "075", "自動車整備": "075", "修理": "075",
    "メンテナンス": "075",
    # 076-079 製品検査工・機械検査工
    "検査": "076", "品質検査": "076", "製品検査": "076", "機械検査": "079",
    # 080 生産関連の職業（塗装・製図を含む）
    "塗装": "080", "CAD": "080", "CADオペレーター": "080", "製図": "080",
    "生産管理": "080", "品質管理": "080",
    # 081 生産類似の職業
    "印刷": "081",

    # ========================================
    # 13: 配送・輸送・機械運転の職業
    # 中分類: 082配送・集荷, 083貨物自動車運転, 084バス運転, 085乗用車運転,
    #         086その他の自動車運転, 087鉄道・船舶・航空機運転, 088その他の輸送,
    #         089施設機械設備操作・建設機械運転
    # ========================================
    # 082 配送・集荷の職業
    "配送": "082", "配達": "082", "集荷": "082", "荷物配達": "082",
    "ルート配送": "082", "宅配": "082", "郵便配達": "082", "新聞配達": "082",
    # 083 貨物自動車運転の職業
    "トラック": "083", "大型トラック": "083", "中型トラック": "083",
    "トレーラー": "083", "ダンプ": "083", "トラック運転手": "083",
    # 084 バス運転の職業
    "バス運転": "084", "路線バス": "084", "送迎バス": "084", "バス": "084",
    # 085 乗用車運転の職業
    "タクシー": "085", "ハイヤー": "085", "介護タクシー": "085", "送迎": "085",
    # 086-088 その他の自動車運転、輸送
    "ドライバー": "083", "運転手": "083", "運転": "086", "運送": "083",
    # 087 鉄道・船舶・航空機運転
    "鉄道運転士": "087", "船長": "087", "航海士": "087", "パイロット": "087",
    # 088 その他の輸送
    "車掌": "088", "フォークリフト": "088",
    # 089 施設機械設備操作・建設機械運転
    "ビル設備管理": "089", "ボイラー": "089", "クレーン": "089",
    "建設機械": "089", "重機": "089", "重機オペレーター": "089",

    # ========================================
    # 14: 建設・土木・電気工事の職業
    # 中分類: 090建設躯体工事, 091建設の職業（躯体工事を除く）, 092土木の職業,
    #         093採掘の職業, 094電気・通信工事の職業
    # ========================================
    # 090 建設躯体工事の職業
    "型枠大工": "090", "とび": "090", "とび工": "090", "鳶": "090",
    "解体": "090", "解体工": "090", "鉄筋工": "090",
    # 091 建設の職業（建設躯体工事の職業を除く）
    "大工": "091", "ブロック積": "091", "タイル張": "091", "屋根": "091",
    "左官": "091", "畳": "091", "配管": "091", "配管工": "091",
    "内装": "091", "内装工": "091", "防水工": "091", "建設": "091",
    # 092 土木の職業
    "土木": "092", "土木作業": "092", "舗装": "092", "トンネル": "092",
    # 093 採掘の職業
    "採掘": "093",
    # 094 電気・通信工事の職業
    "電気工事": "094", "電気工事士": "094", "電工": "094",
    "通信工事": "094", "配線工事": "094", "施工": "091", "現場": "091",

    # ========================================
    # 15: 運搬・清掃・包装・選別等の職業
    # 中分類: 095荷役・運搬作業員, 096清掃・洗浄作業員, 097包装作業員,
    #         098選別・ピッキング作業員, 099その他の運搬・清掃・包装・選別等の職業
    # ========================================
    # 095 荷役・運搬作業員
    "荷役": "095", "運搬": "095", "倉庫": "095", "倉庫作業": "095", "梱包": "095",
    # 096 清掃・洗浄作業員
    "清掃": "096", "ビル清掃": "096", "ハウスクリーニング": "096",
    "客室清掃": "096", "道路清掃": "096", "ゴミ収集": "096", "産業廃棄物": "096",
    "洗浄": "096", "洗車": "096",
    # 097 包装作業員
    "包装": "097", "ラベル貼り": "097", "シール貼り": "097",
    # 098 選別・ピッキング作業員
    "選別": "098", "ピッキング": "098", "仕分け": "098",
    # 099 その他の運搬・清掃・包装・選別等の職業
    "軽作業": "099", "品出し": "099", "陳列": "099", "補充": "099",
    "洗い場": "099", "用務員": "099", "搬入": "095", "搬出": "095", "荷物": "095",
}

# 逆引き用
PREFECTURE_CODE_TO_NAME: Dict[str, str] = {v: k for k, v in PREFECTURE_CODES.items()}
JOB_CATEGORY_CODE_TO_NAME: Dict[str, str] = {v: k for k, v in JOB_CATEGORY_CODES.items()}


class HelloworkScraper(BaseScraper):
    """ハローワーク求人スクレイパー"""

    def __init__(self, config: dict = None):
        super().__init__("hellowork")
        self.source_name = "hellowork"
        self.base_url = "https://www.hellowork.mhlw.go.jp"
        self.search_url = f"{self.base_url}/kensaku/GECA110010.do?action=initDisp&screenId=GECA110010"
        self.logger = logging.getLogger(__name__)
        # リアルタイム件数コールバック
        self._realtime_callback = None

    def set_realtime_callback(self, callback):
        """リアルタイム件数コールバックを設定"""
        self._realtime_callback = callback

    def _report_count(self, count: int):
        """件数を報告"""
        if self._realtime_callback:
            self._realtime_callback(count)

    async def _check_for_error_page(self, page: Page) -> bool:
        """エラーページかどうかをチェック"""
        try:
            page_text = await page.inner_text('body')
            error_indicators = [
                "システムの混雑",
                "続行不可能なエラー",
                "エラーが発生しました",
                "システムエラー",
                "時間をおいて再度",
            ]
            for indicator in error_indicators:
                if indicator in page_text:
                    self.logger.warning(f"ハローワークエラーページ検出: {indicator}")
                    return True
            return False
        except Exception:
            return False

    def _get_prefecture_code(self, area: str) -> Optional[str]:
        """エリア名から都道府県コードを取得"""
        if area in PREFECTURE_CODES:
            return PREFECTURE_CODES[area]
        for name, code in PREFECTURE_CODES.items():
            if name in area or area in name:
                return code
        return None

    def _get_job_category_code(self, keyword: str) -> Optional[str]:
        """キーワードから職業分類コードを取得"""
        # まず完全一致を試す
        if keyword in KEYWORD_TO_CATEGORY:
            return KEYWORD_TO_CATEGORY[keyword]
        # 部分一致を試す
        for kw, code in KEYWORD_TO_CATEGORY.items():
            if kw in keyword or keyword in kw:
                return code
        return None

    async def search(self, page: Page, keyword: str, area: str, max_pages: int = 5) -> List[Dict[str, Any]]:
        """ハローワークで求人を検索

        Args:
            page: Playwrightページオブジェクト
            keyword: 検索キーワード（職業分類コードに変換される）
            area: 検索エリア（都道府県名）
            max_pages: 最大ページ数

        Returns:
            求人リスト
        """
        all_jobs = []
        prefecture_code = self._get_prefecture_code(area)

        if not prefecture_code:
            self.logger.warning(f"都道府県コードが見つかりません: {area}")
            return all_jobs

        # キーワードから職業分類コードを取得
        job_category_code = self._get_job_category_code(keyword)
        if job_category_code:
            self.logger.info(f"キーワード '{keyword}' → 職業分類コード '{job_category_code}'")
        else:
            self.logger.info(f"キーワード '{keyword}' に対応する職業分類コードがありません。フリーワード検索を使用します。")

        try:
            self.logger.info(f"ハローワーク検索開始: {keyword} in {area}")
            await page.goto(self.search_url, wait_until="networkidle", timeout=60000)
            await asyncio.sleep(3)

            # エラーページチェック
            if await self._check_for_error_page(page):
                self.logger.error("ハローワークがエラーページを返しました。時間をおいて再試行してください。")
                return all_jobs

            await self._fill_search_form(page, keyword, prefecture_code, job_category_code)
            await self._submit_search(page)
            await asyncio.sleep(3)

            # 検索後もエラーページチェック
            if await self._check_for_error_page(page):
                self.logger.error("検索後にエラーページが表示されました。")
                return all_jobs

            for page_num in range(1, max_pages + 1):
                self.logger.info(f"ページ {page_num}/{max_pages} を処理中...")

                # ページ内容が読み込まれるまで待機
                try:
                    await page.wait_for_selector('section.card_job, .kyujin_list, table.normal', timeout=10000)
                except PlaywrightTimeout:
                    self.logger.warning(f"ページ {page_num} の求人リスト読み込みタイムアウト")
                    # 少し待ってから続行を試みる
                    await asyncio.sleep(2)

                jobs = await self._extract_job_list(page)
                if not jobs:
                    self.logger.info("これ以上の求人がありません")
                    break

                all_jobs.extend(jobs)
                self.logger.info(f"ページ {page_num}: {len(jobs)}件取得（累計: {len(all_jobs)}件）")
                # リアルタイム件数報告
                self._report_count(len(all_jobs))

                if page_num < max_pages:
                    has_next = await self._go_to_next_page(page)
                    if not has_next:
                        self.logger.info("次のページがありません（最終ページ）")
                        break
                    # ページ遷移後の待機
                    await asyncio.sleep(3)

        except PlaywrightTimeout as e:
            self.logger.error(f"タイムアウト: {e}")
        except Exception as e:
            self.logger.error(f"検索エラー: {e}")

        self.logger.info(f"合計 {len(all_jobs)} 件の求人を取得")
        return all_jobs

    async def _fill_search_form(self, page: Page, keyword: str, prefecture_code: str, job_category_code: Optional[str]):
        """検索フォームに入力

        Args:
            page: Playwrightページオブジェクト
            keyword: 検索キーワード
            prefecture_code: 都道府県コード
            job_category_code: 職業分類コード（Noneの場合はフリーワード検索）
        """
        try:
            # 1. 都道府県選択
            prefecture_select = page.locator('#ID_tDFK1CmbBox')
            if await prefecture_select.count() > 0:
                await prefecture_select.select_option(value=prefecture_code)
                self.logger.info(f"都道府県選択成功: {prefecture_code}")
                await asyncio.sleep(0.5)
            else:
                self.logger.warning("都道府県選択欄が見つかりません")

            # 2. 職業分類コードで検索するか、フリーワードで検索するか
            if job_category_code:
                # 職業分類コードを入力フィールドに入力
                # ID_sKGYBRUIJo1: 職業分類コード上位（大分類）
                # ID_sKGYBRUIGe1: 職業分類コード下位（中分類）→空欄でもOK
                category_input_jo = page.locator('#ID_sKGYBRUIJo1')
                if await category_input_jo.count() > 0:
                    await category_input_jo.fill(job_category_code)
                    self.logger.info(f"職業分類コード入力成功: {job_category_code}")
                else:
                    # 入力欄がない場合は、職種選択ボタンをクリックしてダイアログを開く
                    self.logger.info("職業分類コード入力欄がありません。ボタン経由で選択を試みます。")
                    await self._select_job_category_via_button(page, job_category_code)
            else:
                # フリーワード検索
                freeword_input = page.locator('#ID_freeWordInput')
                if await freeword_input.count() > 0:
                    await freeword_input.fill(keyword)
                    self.logger.info(f"フリーワード入力成功: {keyword}")
                else:
                    self.logger.warning("フリーワード入力欄が見つかりません")

        except Exception as e:
            self.logger.error(f"フォーム入力エラー: {e}")

    async def _select_job_category_via_button(self, page: Page, job_category_code: str):
        """職業分類選択ボタンをクリックしてカテゴリを選択

        Note: ダイアログが開く形式の場合に使用
        """
        try:
            # 「職業分類を選択」ボタンをクリック
            bunrui_buttons = page.locator('button:has-text("職業分類を選択")')
            if await bunrui_buttons.count() > 0:
                await bunrui_buttons.first.click()
                await asyncio.sleep(1)

                # ダイアログ内でカテゴリを選択
                # 具体的なセレクタはサイトの構造に依存
                category_checkbox = page.locator(f'input[value="{job_category_code}"]')
                if await category_checkbox.count() > 0:
                    await category_checkbox.check()
                    self.logger.info(f"職業分類チェックボックス選択: {job_category_code}")

                # OKボタンをクリック
                ok_button = page.locator('input[value="選択"], button:has-text("選択")')
                if await ok_button.count() > 0:
                    await ok_button.first.click()
                    await asyncio.sleep(0.5)
            else:
                self.logger.warning("職業分類選択ボタンが見つかりません")
        except Exception as e:
            self.logger.error(f"職業分類選択エラー: {e}")

    async def _submit_search(self, page: Page):
        """検索を実行"""
        try:
            # 検索ボタン（ID_searchBtn）
            search_button = page.locator('#ID_searchBtn')
            if await search_button.count() > 0:
                await search_button.click()
                self.logger.info("検索ボタンクリック成功")
            else:
                # 代替セレクタ
                alt_button = page.locator('input[type="submit"][value="検索"]')
                if await alt_button.count() > 0:
                    await alt_button.first.click()
                    self.logger.info("検索ボタン（代替）クリック成功")
                else:
                    self.logger.warning("検索ボタンが見つかりません")
                    return

            await page.wait_for_load_state("networkidle", timeout=60000)
            self.logger.info("検索結果ページ読み込み完了")

            # 表示件数を50件に設定
            await self._set_display_count(page, "50")

        except Exception as e:
            self.logger.error(f"検索実行エラー: {e}")

    async def _set_display_count(self, page: Page, count: str = "50"):
        """検索結果の表示件数を設定

        Args:
            page: Playwrightページオブジェクト
            count: 表示件数 ("10", "30", "50")

        HTML構造:
        - 表示件数セレクト: select#ID_fwListNaviDispTop
        - 値: 10, 30, 50
        """
        try:
            # 表示件数セレクトを探す
            disp_select = page.locator('#ID_fwListNaviDispTop')
            if await disp_select.count() > 0:
                current_value = await disp_select.input_value()
                if current_value != count:
                    await disp_select.select_option(value=count)
                    await asyncio.sleep(1)
                    await page.wait_for_load_state("networkidle", timeout=30000)
                    self.logger.info(f"表示件数を{count}件に変更しました")
                else:
                    self.logger.debug(f"表示件数は既に{count}件です")
            else:
                self.logger.debug("表示件数セレクトが見つかりません")
        except Exception as e:
            self.logger.debug(f"表示件数設定エラー: {e}")

    async def _extract_job_list(self, page: Page) -> List[Dict[str, Any]]:
        """検索結果から求人リストを抽出

        HTML構造:
        - 各求人は table.kyujin で囲まれている
        - tr.kyujin_head: ヘッダー行（職種など）
        - tr.kyujin_body: 本体行（会社情報など）
        - tr.kyujin_foot: フッター行（詳細ボタンなど）
        - 詳細リンク: a#ID_dispDetailBtn[href*="kJNo"]
        """
        jobs = []
        seen_job_ids = set()  # 同じjob_idの重複を防ぐ（同一ページ内）

        try:
            # table.kyujin を使って求人カードを取得
            job_tables = page.locator('table.kyujin')
            table_count = await job_tables.count()
            self.logger.info(f"求人テーブル数: {table_count}")

            if table_count > 0:
                for i in range(table_count):
                    try:
                        job_table = job_tables.nth(i)
                        job = await self._extract_job_from_table(job_table, i)

                        if job and job.get('job_id'):
                            job_id = job['job_id']
                            # 同一ページ内の重複チェック
                            if job_id in seen_job_ids:
                                self.logger.debug(f"同一ページ内重複スキップ: {job_id}")
                                continue
                            seen_job_ids.add(job_id)
                            jobs.append(job)
                            self.logger.info(f"求人抽出成功 [{i+1}/{table_count}]: {job_id}")
                    except Exception as e:
                        self.logger.debug(f"テーブル {i} の抽出エラー: {e}")
            else:
                # フォールバック: 直接詳細リンクを探す
                self.logger.info("table.kyujinが見つかりません。詳細リンクを直接検索します。")
                detail_links = page.locator('a[href*="dispDetailBtn"][href*="kJNo"]')
                link_count = await detail_links.count()
                self.logger.info(f"詳細リンク数: {link_count}")

                for i in range(link_count):
                    try:
                        link = detail_links.nth(i)
                        href = await link.get_attribute('href')
                        if not href or 'kJNo' not in href:
                            continue

                        job = await self._extract_job_from_detail_btn(link, href, page)
                        if job and job.get('job_id'):
                            job_id = job['job_id']
                            if job_id in seen_job_ids:
                                continue
                            seen_job_ids.add(job_id)
                            jobs.append(job)
                            self.logger.info(f"求人抽出成功 [{i+1}/{link_count}]: {job_id}")
                    except Exception as e:
                        self.logger.debug(f"リンク {i} の抽出エラー: {e}")

            if jobs:
                self.logger.info(f"合計 {len(jobs)} 件の求人を抽出")
            else:
                self.logger.warning("求人が見つかりません")
                try:
                    body_text = await page.locator('body').inner_text()
                    self.logger.info(f"ページテキスト (先頭500文字): {body_text[:500]}")
                except:
                    pass

        except Exception as e:
            self.logger.error(f"求人リスト抽出エラー: {e}")
        return jobs

    async def _extract_job_from_table(self, job_table, index: int) -> Optional[Dict[str, Any]]:
        """table.kyujin から求人情報を抽出

        HTML構造:
        - tr.kyujin_head: 職種名など
        - tr.kyujin_body: 会社名、勤務地、賃金などがテーブル形式で配置
          - 各行: td.fb.in_width_9em (ラベル) + td (値)
        - tr.kyujin_foot: 詳細ボタン (a#ID_dispDetailBtn)
        """
        try:
            # 詳細リンクからjob_idを取得
            detail_link = job_table.locator('a[href*="kJNo"]')
            if await detail_link.count() == 0:
                self.logger.debug(f"テーブル {index}: 詳細リンクなし")
                return None

            href = await detail_link.first.get_attribute('href')
            if not href:
                return None

            # kJNo を抽出
            match = re.search(r'kJNo=([0-9A-Za-z\-]+)', href)
            if not match:
                self.logger.debug(f"テーブル {index}: kJNoが見つかりません")
                return None

            job_id = match.group(1)

            # 職種名（kyujin_headから）
            title = ""
            head_row = job_table.locator('tr.kyujin_head')
            if await head_row.count() > 0:
                try:
                    # 職種は「職種」ラベルの次のtdにある
                    title_cell = head_row.locator('td.m13, td.fs1').first
                    if await title_cell.count() > 0:
                        title = (await title_cell.inner_text()).strip()[:100]
                    else:
                        # フォールバック: テキストから抽出
                        head_text = await head_row.inner_text()
                        lines = [l.strip() for l in head_text.split('\n') if l.strip()]
                        for line in lines:
                            if len(line) > 2 and '職種' not in line and '求人番号' not in line:
                                title = line[:100]
                                break
                except Exception as e:
                    self.logger.debug(f"職種取得エラー: {e}")

            # kyujin_body内のテーブルから情報を抽出
            company = ""
            location = ""
            salary = ""
            employment_type = ""
            working_hours = ""
            holidays = ""
            age_limit = ""
            job_description = ""

            body_row = job_table.locator('tr.kyujin_body')
            if await body_row.count() > 0:
                try:
                    # 内部テーブルの各行を解析
                    inner_rows = body_row.locator('tr.border_new')
                    row_count = await inner_rows.count()

                    for i in range(row_count):
                        row = inner_rows.nth(i)
                        try:
                            # ラベル（td.fb）と値（次のtd）を取得
                            label_cell = row.locator('td.fb').first
                            value_cell = row.locator('td').nth(1)

                            if await label_cell.count() == 0 or await value_cell.count() == 0:
                                continue

                            label = (await label_cell.inner_text()).strip()
                            value = (await value_cell.inner_text()).strip()

                            # ラベルに応じて値を格納
                            if '事業所名' in label:
                                company = value[:200]
                            elif '就業場所' in label:
                                location = value[:200]
                            elif '賃金' in label:
                                salary = value[:100]
                            elif '雇用形態' in label:
                                employment_type = value[:50]
                            elif '就業時間' in label:
                                working_hours = value[:100]
                            elif '休日' in label:
                                holidays = value[:100]
                            elif '年齢' in label:
                                age_limit = value[:50]
                            elif '仕事の内容' in label:
                                job_description = value[:500]

                        except Exception as e:
                            self.logger.debug(f"行 {i} の解析エラー: {e}")
                            continue

                except Exception as e:
                    self.logger.debug(f"本体情報取得エラー: {e}")

            # 求人番号表示（01010-45772151形式）
            job_number_display = ""
            try:
                table_text = await job_table.inner_text()
                no_match = re.search(r'求人番号[:\s]*(\d{5}-\d{8})', table_text)
                if no_match:
                    job_number_display = no_match.group(1)
            except:
                pass

            return {
                "job_id": job_id,
                "job_number_display": job_number_display,
                "title": title,
                "company": company,
                "location": location,
                "salary": salary,
                "employment_type": employment_type,
                "working_hours": working_hours,
                "holidays": holidays,
                "age_limit": age_limit,
                "job_description": job_description,
                "url": self._build_detail_url(job_id),
                "source": self.source_name,
            }

        except Exception as e:
            self.logger.debug(f"テーブル {index} 抽出エラー: {e}")
            return None

    def _build_detail_url(self, job_id: str) -> str:
        """job_id (kJNo) から詳細ページURLを構築

        Args:
            job_id: 求人番号 (kJNo)。例: 0804021563451

        Returns:
            詳細ページURL
            例: https://www.hellowork.mhlw.go.jp/kensaku/GECA110010.do?screenId=GECA110010&action=dispDetailBtn&kJNo=0804021563451&kJKbn=1
        """
        return f"{self.base_url}/kensaku/GECA110010.do?screenId=GECA110010&action=dispDetailBtn&kJNo={job_id}&kJKbn=1"

    async def _extract_job_from_detail_btn(self, link, href: str, page: Page) -> Optional[Dict[str, Any]]:
        """詳細ボタンから求人情報を抽出"""
        try:
            # URLから求人番号を抽出（kJNo パラメータ）
            # 例: kJNo=0804021563451
            # kJNoは13桁の数字（都道府県コード2桁 + 職業分類コード2桁 + 求人番号9桁）
            job_id = None
            match = re.search(r'kJNo=([0-9A-Za-z\-]+)', href)
            if match:
                job_id = match.group(1)

            if not job_id:
                self.logger.debug(f"求人番号が見つかりません: {href[:100]}")
                return None

            # 詳細URLを構築（kJNoを使った固定フォーマット）
            detail_url = self._build_detail_url(job_id)

            # リンクの親要素から情報を取得
            title = ""
            company = ""
            location = ""
            salary = ""

            try:
                parent_row = link.locator('xpath=ancestor::tr[1]')
                if await parent_row.count() > 0:
                    row_text = await parent_row.inner_text()
                    lines = [l.strip() for l in row_text.split('\n') if l.strip()]
                    if lines:
                        for line in lines:
                            if not title and len(line) > 2 and '詳細' not in line:
                                title = line[:100]
                                break

                parent_cell = link.locator('xpath=ancestor::td[1]')
                if await parent_cell.count() > 0:
                    parent_row = link.locator('xpath=ancestor::tr[1]')
                    if await parent_row.count() > 0:
                        cells = parent_row.locator('td')
                        cell_count = await cells.count()

                        cell_texts = []
                        for i in range(cell_count):
                            try:
                                cell_text = (await cells.nth(i).inner_text()).strip()
                                cell_texts.append(cell_text)
                            except:
                                cell_texts.append("")

                        for i, text in enumerate(cell_texts):
                            if '詳細を表示' in text:
                                continue
                            if not company and ('株式会社' in text or '有限会社' in text or '合同会社' in text):
                                company = text[:100]
                            elif not title and len(text) > 3:
                                title = text[:100]
                            elif not location and ('県' in text or '都' in text or '府' in text or '道' in text):
                                location = text[:100]
                            elif not salary and ('円' in text or '万' in text):
                                salary = text[:50]

            except Exception as e:
                self.logger.debug(f"親要素からの情報取得エラー: {e}")

            return {
                "job_id": job_id,
                "title": title,
                "company": company,
                "location": location,
                "salary": salary,
                "url": detail_url,
                "source": self.source_name,
            }
        except Exception as e:
            self.logger.debug(f"詳細ボタン情報抽出エラー: {e}")
            return None

    async def _go_to_next_page(self, page: Page) -> bool:
        """次のページへ移動

        HTML構造:
        - ページネーションは ul.flex.page_navi 内にある（ページ上部と下部の2箇所）
        - 次へボタン: input[name="fwListNaviBtnNext"][value="次へ＞"]
        - ボタンは type="submit" なのでフォーム送信によりページ遷移
        """
        try:
            # 「次へ＞」ボタンを探す（上部と下部に2つあるので first を使用）
            next_button = page.locator('input[name="fwListNaviBtnNext"]').first
            if await next_button.count() > 0:
                # ボタンが無効化されていないかチェック
                is_disabled = await next_button.is_disabled()
                if is_disabled:
                    self.logger.info("次へボタンが無効化されています（最終ページ）")
                    return False

                # クリック前の現在のページ番号を取得（ページ遷移確認用）
                current_disabled = page.locator('ul.page_navi input[disabled]').first
                current_page_value = ""
                if await current_disabled.count() > 0:
                    current_page_value = await current_disabled.get_attribute("value") or ""
                    self.logger.debug(f"現在のページ: {current_page_value}")

                # クリックしてページ遷移を待つ
                await next_button.click()

                # ページ遷移を確実に待つ
                await page.wait_for_load_state("domcontentloaded", timeout=30000)
                await asyncio.sleep(2)  # 追加の待機

                # ページが変わったことを確認
                new_disabled = page.locator('ul.page_navi input[disabled]').first
                if await new_disabled.count() > 0:
                    new_page_value = await new_disabled.get_attribute("value") or ""
                    self.logger.info(f"ページ遷移成功: {current_page_value} → {new_page_value}")
                else:
                    self.logger.info("次のページへ移動成功")

                return True

            # フォールバック: テキストで探す
            fallback_button = page.locator('input[value*="次へ"]').first
            if await fallback_button.count() > 0:
                is_disabled = await fallback_button.is_disabled()
                if is_disabled:
                    self.logger.info("次へボタンが無効化されています（最終ページ）")
                    return False

                await fallback_button.click()
                await page.wait_for_load_state("domcontentloaded", timeout=30000)
                await asyncio.sleep(2)
                self.logger.info("次のページへ移動成功（フォールバック）")
                return True

            self.logger.info("次へボタンが見つかりません（最終ページの可能性）")
            return False

        except Exception as e:
            self.logger.warning(f"次ページ移動エラー: {e}")
        return False

    async def extract_detail_info(self, page: Page, job_url_or_id: str) -> Dict[str, Any]:
        """求人詳細を取得（抽象メソッド実装）

        Args:
            page: Playwrightページオブジェクト
            job_url_or_id: 詳細ページURLまたはjob_id (kJNo)
                - URLの場合: そのままアクセス
                - job_idの場合: _build_detail_urlでURLを構築

        Returns:
            詳細情報の辞書
        """
        detail = {}
        try:
            # URLかjob_idかを判定
            if job_url_or_id.startswith('http'):
                job_url = job_url_or_id
            else:
                # job_idからURLを構築
                job_url = self._build_detail_url(job_url_or_id)

            await page.goto(job_url, wait_until="networkidle", timeout=60000)
            await asyncio.sleep(3)

            body_text = await page.inner_text('body')

            # 求人番号
            job_no_match = re.search(r'求人番号[:\s]*(\d+-\d+)', body_text)
            if job_no_match:
                detail["job_number"] = job_no_match.group(1)

            # セクションごとに情報を抽出
            async def extract_section(label: str) -> str:
                try:
                    selectors = [
                        f'text="{label}" >> xpath=following-sibling::*[1]',
                        f'//*[contains(text(), "{label}")]/following-sibling::*[1]',
                    ]
                    for selector in selectors:
                        try:
                            elem = page.locator(selector)
                            if await elem.count() > 0:
                                return (await elem.first.inner_text()).strip()
                        except:
                            continue

                    pattern = rf'{re.escape(label)}[:\s]*([\s\S]*?)(?=\n\n|\n[▼■●]|$)'
                    match = re.search(pattern, body_text)
                    if match:
                        return match.group(1).strip()[:500]
                except Exception as e:
                    self.logger.debug(f"セクション抽出エラー ({label}): {e}")
                return ""

            # 事業所名
            detail["company_name"] = await extract_section("事業所名")
            if not detail["company_name"]:
                match = re.search(r'事業所名[:\s]*([^\n]+)', body_text)
                if match:
                    detail["company_name"] = match.group(1).strip()

            # 職種
            detail["job_title"] = await extract_section("職種")
            if not detail["job_title"]:
                match = re.search(r'職種[:\s]*([^\n]+)', body_text)
                if match:
                    detail["job_title"] = match.group(1).strip()

            # 仕事内容
            detail["job_description"] = await extract_section("仕事内容")
            if not detail["job_description"]:
                match = re.search(r'仕事内容[:\s]*([\s\S]*?)(?=雇用形態|$)', body_text)
                if match:
                    detail["job_description"] = match.group(1).strip()[:1000]

            # 雇用形態
            detail["employment_type"] = await extract_section("雇用形態")
            if not detail["employment_type"]:
                if "正社員" in body_text:
                    detail["employment_type"] = "正社員"
                elif "パート" in body_text:
                    detail["employment_type"] = "パート"
                elif "契約社員" in body_text:
                    detail["employment_type"] = "契約社員"

            # 賃金・給与
            salary_match = re.search(r'(\d{2,3},?\d{3}円?[〜～\-]\d{2,3},?\d{3}円?)', body_text)
            if salary_match:
                detail["salary"] = salary_match.group(1)
            else:
                detail["salary"] = await extract_section("賃金")

            # 就業場所・勤務地
            detail["work_location"] = await extract_section("就業場所")
            if not detail["work_location"]:
                match = re.search(r'就業場所[:\s]*([^\n]+(?:\n[^\n▼■]+)*)', body_text)
                if match:
                    detail["work_location"] = match.group(1).strip()[:300]

            # 就業時間
            detail["working_hours"] = await extract_section("就業時間")
            if not detail["working_hours"]:
                time_match = re.search(r'(\d{1,2}時\d{2}分[〜～\-]\d{1,2}時\d{2}分)', body_text)
                if time_match:
                    detail["working_hours"] = time_match.group(1)

            # 休日
            detail["holidays"] = await extract_section("休日")
            if not detail["holidays"]:
                holiday_match = re.search(r'年間休日[:\s]*(\d+日?)', body_text)
                if holiday_match:
                    detail["holidays"] = f"年間休日 {holiday_match.group(1)}"

            # 学歴
            detail["education"] = await extract_section("学歴")
            if not detail["education"]:
                if "大学以上" in body_text:
                    detail["education"] = "大学以上"
                elif "高校以上" in body_text:
                    detail["education"] = "高校以上"
                elif "不問" in body_text:
                    detail["education"] = "不問"

            # 必要な資格・免許
            detail["required_license"] = await extract_section("必要な免許")
            if not detail["required_license"]:
                if "普通自動車" in body_text:
                    detail["required_license"] = "普通自動車運転免許"

            # 必要な経験
            detail["required_experience"] = await extract_section("必要な経験")

            # 年齢
            age_match = re.search(r'年齢[:\s]*(\d+歳[^\n]*)', body_text)
            if age_match:
                detail["age_limit"] = age_match.group(1)

            self.logger.info(f"詳細取得成功: 会社名={detail.get('company_name', '')[:20]}, 職種={detail.get('job_title', '')[:20]}")

        except PlaywrightTimeout as e:
            self.logger.error(f"詳細取得タイムアウト: {e}")
        except Exception as e:
            self.logger.error(f"詳細取得エラー: {e}")
        return detail

    async def scrape_with_details(
        self,
        page: Page,
        keyword: str,
        area: str,
        max_pages: int = 5,
        fetch_details: bool = True,
        existing_job_ids: set = None,
    ) -> List[Dict[str, Any]]:
        """求人検索と詳細取得を実行

        Args:
            page: Playwrightページオブジェクト
            keyword: 検索キーワード（職業分類コードに変換される）
            area: 検索エリア（都道府県名）
            max_pages: 最大ページ数
            fetch_details: 詳細ページを取得するか
            existing_job_ids: DB内の既存job_idセット（これらはスキップ）

        Note:
            重複チェックは _extract_job_list 内で同一ページ内のみ行う。
            異なる検索条件で同じ求人が出現するのは正常なので、
            セッション全体での重複チェックは行わない。
        """
        if existing_job_ids is None:
            existing_job_ids = set()

        jobs = await self.search(page, keyword, area, max_pages)
        if not jobs:
            return []

        # DB既存チェックのみ実行
        jobs_to_fetch = []
        existing_count = 0

        for job in jobs:
            job_id = job.get("job_id")
            if not job_id:
                jobs_to_fetch.append(job)
                continue

            if job_id in existing_job_ids:
                existing_count += 1
                self.logger.debug(f"既存スキップ: {job_id}")
                continue

            jobs_to_fetch.append(job)

        if existing_count > 0:
            self.logger.info(f"既存スキップ: {existing_count}件")

        self.logger.info(f"詳細取得対象: {len(jobs_to_fetch)}件（検索結果: {len(jobs)}件）")

        if fetch_details and jobs_to_fetch:
            self.logger.info(f"{len(jobs_to_fetch)}件の詳細を取得中...")
            for i, job in enumerate(jobs_to_fetch):
                job_id = job.get("job_id")
                if job_id:
                    try:
                        # job_idから直接詳細URLを構築してアクセス
                        detail = await self.extract_detail_info(page, job_id)
                        job.update(detail)
                        # URLも正規化されたものに更新
                        job["url"] = self._build_detail_url(job_id)
                        self.logger.debug(f"詳細取得 ({i+1}/{len(jobs_to_fetch)}): {job_id}")
                    except Exception as e:
                        self.logger.debug(f"詳細取得失敗: {job_id}: {e}")
                    await asyncio.sleep(1)

        if jobs_to_fetch:
            jobs_to_fetch[0]["_meta"] = {
                "existing_count": existing_count,
            }

        return jobs_to_fetch
