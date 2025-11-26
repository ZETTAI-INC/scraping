"""
Modern QSS Styles for the Job Collector Application
Premium Design: Slate & Blue Theme (High Readability)
"""

MODERN_STYLE = """
/* Global Reset */
QWidget {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    font-size: 15px; /* Increased from 13px */
    color: #0f172a; /* Slate 900 - Darker for better contrast */
    background-color: #f8fafc; /* Slate 50 */
}

/* Main Window */
QMainWindow {
    background-color: #f8fafc;
}

/* GroupBox - Card Style */
QGroupBox {
    background-color: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    margin-top: 28px; /* Increased spacing */
    padding-top: 28px;
    padding-bottom: 16px;
    padding-left: 16px;
    padding-right: 16px;
    font-weight: 600;
    color: #334155;
}

QGroupBox::title {
    subcontrol-origin: margin;
    subcontrol-position: top left;
    padding: 0 10px;
    left: 12px;
    background-color: #ffffff;
    color: #0f172a;
    font-size: 16px; /* Larger title */
    font-weight: 700;
}

/* Buttons - Premium Feel */
QPushButton {
    background-color: #ffffff;
    border: 1px solid #cbd5e1;
    border-radius: 8px;
    padding: 12px 24px; /* Larger touch target */
    color: #475569;
    font-weight: 600;
    font-size: 14px;
}

QPushButton:hover {
    background-color: #f1f5f9;
    border-color: #94a3b8;
    color: #1e293b;
}

QPushButton:pressed {
    background-color: #e2e8f0;
    padding-top: 13px;
    padding-bottom: 11px;
}

QPushButton:checked {
    background-color: #eff6ff;
    border-color: #3b82f6;
    color: #2563eb;
}

/* Primary Button */
QPushButton[class="primary"] {
    background-color: #3b82f6;
    border: 1px solid #2563eb;
    color: white;
    font-size: 15px;
    font-weight: 700;
}

QPushButton[class="primary"]:hover {
    background-color: #2563eb;
    border-color: #1d4ed8;
}

QPushButton[class="primary"]:pressed {
    background-color: #1d4ed8;
}

/* Success Button */
QPushButton[class="success"] {
    background-color: #10b981;
    border: 1px solid #059669;
    color: white;
    font-size: 15px;
    font-weight: 700;
}

QPushButton[class="success"]:hover {
    background-color: #059669;
}

/* Input Fields */
QLineEdit, QSpinBox, QTextEdit {
    background-color: #ffffff;
    border: 1px solid #cbd5e1;
    border-radius: 8px;
    padding: 12px; /* More breathing room */
    color: #0f172a;
    font-size: 15px;
    selection-background-color: #3b82f6;
    selection-color: white;
}

QLineEdit:focus, QSpinBox:focus, QTextEdit:focus {
    border: 2px solid #3b82f6;
    padding: 11px;
    background-color: #ffffff;
}

QLineEdit:hover, QSpinBox:hover, QTextEdit:hover {
    border-color: #94a3b8;
}

/* CheckBox */
QCheckBox {
    spacing: 12px;
    padding: 6px;
    color: #334155;
    font-size: 15px;
}

QCheckBox::indicator {
    width: 22px; /* Larger checkbox */
    height: 22px;
    border: 1px solid #cbd5e1;
    border-radius: 6px;
    background-color: #ffffff;
}

QCheckBox::indicator:hover {
    border-color: #94a3b8;
}

QCheckBox::indicator:checked {
    background-color: #3b82f6;
    border-color: #3b82f6;
    image: none;
}

/* ScrollArea */
QScrollArea {
    border: none;
    background-color: transparent;
}

QScrollArea > QWidget > QWidget {
    background-color: transparent;
}

/* TabWidget */
QTabWidget::pane {
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    background-color: #ffffff;
    top: -1px;
}

QTabBar::tab {
    background-color: #f1f5f9;
    border: 1px solid #e2e8f0;
    border-bottom: none;
    border-top-left-radius: 8px;
    border-top-right-radius: 8px;
    padding: 14px 28px; /* Larger tabs */
    margin-right: 4px;
    color: #64748b;
    font-weight: 600;
    font-size: 14px;
}

QTabBar::tab:selected {
    background-color: #ffffff;
    border-bottom: 1px solid #ffffff;
    color: #3b82f6;
}

QTabBar::tab:hover {
    background-color: #f8fafc;
    color: #475569;
}

/* TableWidget */
QTableWidget {
    background-color: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    gridline-color: #f1f5f9;
    selection-background-color: #eff6ff;
    selection-color: #1e3a8a;
    alternate-background-color: #f8fafc;
    outline: none;
    font-size: 14px;
}

QHeaderView::section {
    background-color: #f1f5f9;
    padding: 16px; /* More padding in header */
    border: none;
    border-bottom: 1px solid #cbd5e1;
    font-weight: 700;
    color: #475569;
    text-transform: uppercase;
    font-size: 13px;
    letter-spacing: 0.5px;
}

QTableCornerButton::section {
    background-color: #f1f5f9;
    border: none;
    border-bottom: 1px solid #cbd5e1;
}

/* Splitter */
QSplitter::handle {
    background-color: #e2e8f0;
    width: 1px;
    margin: 4px;
}

/* ProgressBar */
QProgressBar {
    border: none;
    border-radius: 6px;
    background-color: #e2e8f0;
    text-align: center;
    height: 12px; /* Thicker progress bar */
    color: transparent;
}

QProgressBar::chunk {
    background-color: #3b82f6;
    border-radius: 6px;
}

/* StatusBar */
QStatusBar {
    background-color: #ffffff;
    border-top: 1px solid #e2e8f0;
    color: #64748b;
    padding: 8px;
    font-size: 13px;
}

/* ScrollBar (Vertical) */
QScrollBar:vertical {
    border: none;
    background: #f1f5f9;
    width: 12px; /* Thicker scrollbar */
    margin: 0px;
    border-radius: 6px;
}

QScrollBar::handle:vertical {
    background: #cbd5e1;
    min-height: 20px;
    border-radius: 6px;
}

QScrollBar::handle:vertical:hover {
    background: #94a3b8;
}

QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
    height: 0px;
}

/* ScrollBar (Horizontal) */
QScrollBar:horizontal {
    border: none;
    background: #f1f5f9;
    height: 12px;
    margin: 0px;
    border-radius: 6px;
}

QScrollBar::handle:horizontal {
    background: #cbd5e1;
    min-width: 20px;
    border-radius: 6px;
}

QScrollBar::handle:horizontal:hover {
    background: #94a3b8;
}

QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {
    width: 0px;
}
"""
