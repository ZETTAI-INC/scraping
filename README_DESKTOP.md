# 求人情報自動収集システム - デスクトップ版

タウンワークから求人情報を自動収集し、SQLiteデータベースに保存、フィルタリング後にCSV出力できるWindowsデスクトップアプリケーションです。

## 機能一覧

### 1. 求人情報収集（タウンワーク）
- キーワード検索
- 地域指定
- 複数ページの自動取得
- 並列処理による高速化

### 2. データベース管理（SQLite）
- 求人情報の永続化
- 重複チェック・更新管理
- 新着フラグ管理

### 3. フィルタリング機能
- 電話番号重複削除
- 従業員数フィルタ（1,001人以上除外）
- 派遣・紹介キーワード除外
- 業界フィルタ（広告・メディア等）
- 勤務地フィルタ（沖縄除外）
- 電話番号プレフィックスフィルタ

### 4. CSV出力
- UTF-8 BOM付き（Excel対応）
- 日本語ヘッダー
- フィルタ適用前/後の出力

### 5. 新着監視・スケジューラー
- 定期自動クローリング（30分〜24時間間隔）
- 実行時間帯制限
- 新着通知

## インストール

### 必要環境
- Python 3.9以上
- Windows 10/11（exe版）

### セットアップ

```bash
# リポジトリをクローン
git clone https://github.com/your-repo/job-collector.git
cd job-collector

# 仮想環境を作成
python -m venv venv

# 仮想環境を有効化
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# 依存パッケージをインストール
pip install -r requirements.txt

# Playwrightのブラウザをインストール
playwright install chromium
```

## 使い方

### デスクトップGUIの起動

```bash
python main.py
```

### CLI版の実行

```bash
python -m src.services.crawl_service
```

## ディレクトリ構成

```
job-collector/
├── main.py                 # メインエントリーポイント
├── requirements.txt        # 依存パッケージ
├── build_exe.spec         # PyInstaller設定
├── build.bat              # Windowsビルドスクリプト
├── build.sh               # Mac/Linuxビルドスクリプト
│
├── src/
│   ├── database/          # データベース層
│   │   ├── db_manager.py      # SQLite管理
│   │   └── job_repository.py  # 求人リポジトリ
│   │
│   ├── filters/           # フィルタリング
│   │   └── job_filter.py      # 除外ルール実装
│   │
│   ├── services/          # ビジネスロジック
│   │   ├── crawl_service.py   # クロールサービス
│   │   ├── csv_exporter.py    # CSV出力
│   │   └── scheduler_service.py # スケジューラー
│   │
│   └── gui/               # デスクトップGUI
│       └── main_window.py     # メインウィンドウ
│
├── scrapers/              # スクレイパー（既存）
│   ├── base_scraper.py
│   └── townwork.py
│
├── config/                # 設定ファイル
│   └── selectors.json
│
├── data/
│   ├── db/               # SQLiteデータベース
│   ├── output/           # CSV出力先
│   └── logs/             # ログファイル
│
└── utils/                 # ユーティリティ（既存）
```

## exe化（Windows向け）

```bash
# Windows
build.bat

# Mac/Linux
./build.sh
```

出力先: `dist/JobCollector.exe`

## フィルタリングルール

CSV出力時に以下の条件で自動フィルタリングされます：

1. **電話番号重複削除**: 同一電話番号の求人は1件のみ保持
2. **従業員数フィルタ**: 1,001人以上の企業を除外
3. **キーワードフィルタ**: 人材派遣、人材紹介等を含む企業を除外
4. **業界フィルタ**: 広告、メディア、出版業界を除外
5. **勤務地フィルタ**: 沖縄県の求人を除外
6. **電話番号プレフィックス**: 0120, 050, 沖縄局番等を除外

## CSV出力形式

```csv
媒体名,求人番号,会社名,会社名カナ,郵便番号,住所1,住所2,住所3,電話番号,FAX番号,職種,雇用形態,給与,勤務時間,休日,就業場所,事業内容,仕事内容,応募資格,採用人数,担当者名,担当者メールアドレス,ページURL,従業員数,取得日時
```

- エンコーディング: UTF-8 (BOM付き)
- 区切り文字: カンマ
- 改行コード: CRLF

## ライセンス

MIT License

## 注意事項

- 各求人媒体の利用規約を確認の上、ご利用ください
- 過度なアクセスは避け、適切な間隔を空けてください
- 取得したデータの取り扱いは個人情報保護法に準拠してください
