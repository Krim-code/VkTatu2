import sys
import os
from PyQt5.QtWidgets import (QApplication, QWidget, QVBoxLayout, QPushButton, QLabel, QListWidget, QLineEdit,
                             QHBoxLayout, QTextEdit, QProgressBar, QComboBox, QMessageBox)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
import sqlite3
import pandas as pd
import requests

def resource_path(relative_path):
    """ Получает путь к ресурсам, работает как в режиме .exe, так и для скрипта """
    try:
        # PyInstaller создает временную папку для хранения ресурсов
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    
    return os.path.join(base_path, relative_path)

class VKParserWorker(QThread):
    update_progress = pyqtSignal(int)
    log_message = pyqtSignal(str)
    parsing_complete = pyqtSignal()

    def __init__(self, group_ids, user_ids, token):
        super().__init__()
        self.group_ids = group_ids
        self.user_ids = user_ids
        self.token = token

    def run(self):
        total_tasks = len(self.group_ids) + len(self.user_ids)
        completed_tasks = 0
        self.update_old_users()

        for group_id in self.group_ids:
            self.log_message.emit(f"Начало парсинга группы {group_id}")
           
            self.get_all_group_members(group_id)
            completed_tasks += 1
            self.update_progress.emit(int((completed_tasks / total_tasks) * 100))

        for user_id in self.user_ids:
            self.log_message.emit(f"Начало парсинга друзей и фолловеров пользователя {user_id}")
            
            self.get_user_friends(user_id)
            self.get_user_followers(user_id)
            completed_tasks += 1
            self.update_progress.emit(int((completed_tasks / total_tasks) * 100))

        self.parsing_complete.emit()

    def update_old_users(self):
        """
        Обновляем статус всех пользователей со статусом 'new' на 'old'
        """
        conn = sqlite3.connect(resource_path('vk_data.db'))
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET status='old' WHERE status='new'")
        conn.commit()
        conn.close()

    def get_all_group_members(self, group_id):
        conn = sqlite3.connect(resource_path('vk_data.db'))
        cursor = conn.cursor()

        url = "https://api.vk.com/method/groups.getMembers"
        params = {
            "group_id": group_id,
            "fields": "id, deactivated, sex",
            "access_token": self.token,
            "v": "5.131"
        }
        
        members = []
        offset = 0
        count = 1000  # Максимальное количество участников за запрос

        while True:
            params['offset'] = offset
            response = requests.get(url, params=params)
            data = response.json()

            if 'error' in data:
                break

            members_batch = data['response']['items']
            for member in members_batch:
                if 'deactivated' not in member:  # Фильтруем деактивированных
                    vk_id = member['id']
                    sex = member.get('sex', 0)
                    members.append((vk_id, sex))  # Собираем всех активных пользователей

            if len(members_batch) < count:
                break

            offset += count

        # Пакетная вставка активных пользователей
        cursor.executemany("INSERT OR IGNORE INTO users (vk_id, sex, status) VALUES (?, ?, 'new')", members)
        conn.commit()
        conn.close()


    def get_user_friends(self, user_id):
        conn = sqlite3.connect(resource_path('vk_data.db'))
        cursor = conn.cursor()

        url = "https://api.vk.com/method/friends.get"
        params = {
            "user_id": user_id,
            "access_token": self.token,
            "v": "5.131",
            "fields": "sex, deactivated"
        }

        response = requests.get(url, params=params)
        data = response.json()

        if 'response' in data:
            friends = []
            for friend in data['response']['items']:
                if 'deactivated' not in friend:  # Фильтруем деактивированных
                    vk_id = friend['id']
                    sex = friend.get('sex', 0)
                    friends.append((vk_id, sex))

            # Пакетная вставка активных друзей
            cursor.executemany("INSERT OR IGNORE INTO users (vk_id, sex, status) VALUES (?, ?, 'new')", friends)

        conn.commit()
        conn.close()

    def get_user_followers(self, user_id):
        conn = sqlite3.connect(resource_path('vk_data.db'))
        cursor = conn.cursor()

        url = "https://api.vk.com/method/users.getFollowers"
        count = 1000
        offset = 0
        followers = []

        while True:
            params = {
                "user_id": user_id,
                "access_token": self.token,
                "v": "5.131",
                "count": count,
                "offset": offset,
                "fields": "sex, deactivated"
            }

            response = requests.get(url, params=params)
            data = response.json()

            if 'error' in data:
                break

            followers_batch = data["response"]["items"]
            for follower in followers_batch:
                if 'deactivated' not in follower:  # Фильтруем деактивированных
                    vk_id = follower['id']
                    sex = follower.get('sex', 0)
                    followers.append((vk_id, sex))

            if len(followers_batch) < count:
                break

            offset += count

        # Пакетная вставка активных подписчиков
        cursor.executemany("INSERT OR IGNORE INTO users (vk_id, sex, status) VALUES (?, ?, 'new')", followers)
        conn.commit()
        conn.close()


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("VK Parser")
        self.resize(600, 700)

        # Устанавливаем тёмную тему
        self.set_apple_style_theme()

        self.conn = sqlite3.connect(resource_path('vk_data.db'))
        self.create_tables()

        # Layouts
        layout = QVBoxLayout()

        # Token input
        self.token_input = QLineEdit(self)
        self.token_input.setPlaceholderText("Введите VK Token")
        layout.addWidget(QLabel("VK Token:"))
        layout.addWidget(self.token_input)

        # Group IDs list
        layout.addWidget(QLabel("Group IDs:"))
        self.group_list = QListWidget(self)
        self.load_group_ids()
        layout.addWidget(self.group_list)

        # User IDs list
        layout.addWidget(QLabel("User IDs:"))
        self.user_list = QListWidget(self)
        self.load_user_ids()
        layout.addWidget(self.user_list)

        # Buttons to add and remove group/user IDs
        add_remove_layout = QHBoxLayout()
        self.add_group_button = QPushButton("Добавить группу", self)
        self.remove_group_button = QPushButton("Удалить группу", self)
        add_remove_layout.addWidget(self.add_group_button)
        add_remove_layout.addWidget(self.remove_group_button)
        layout.addLayout(add_remove_layout)

        self.add_user_button = QPushButton("Добавить пользователя", self)
        self.remove_user_button = QPushButton("Удалить пользователя", self)
        add_remove_layout.addWidget(self.add_user_button)
        add_remove_layout.addWidget(self.remove_user_button)

        # Parsing button
        self.parse_button = QPushButton("Начать парсинг", self)
        layout.addWidget(self.parse_button)

        # Progress bar
        self.progress_bar = QProgressBar(self)
        layout.addWidget(self.progress_bar)

        # Logs area
        self.log_area = QTextEdit(self)
        self.log_area.setReadOnly(True)
        layout.addWidget(QLabel("Логи:"))
        layout.addWidget(self.log_area)

        # CSV generation dropdown
        layout.addWidget(QLabel("Генерация CSV:"))
        self.csv_type_select = QComboBox(self)
        self.csv_type_select.addItems(["Все пользователи", "Мужчины", "Женщины", "Новые пользователи", "Новые мужчины", "Новые женщины"])
        layout.addWidget(self.csv_type_select)

        self.generate_csv_button = QPushButton("Сгенерировать CSV", self)
        layout.addWidget(self.generate_csv_button)

        # Statistics labels
        self.stats_label = QLabel("Статистика")
        layout.addWidget(self.stats_label)

        self.setLayout(layout)

        # Connect buttons to actions
        self.add_group_button.clicked.connect(self.add_group)
        self.remove_group_button.clicked.connect(self.remove_group)
        self.add_user_button.clicked.connect(self.add_user)
        self.remove_user_button.clicked.connect(self.remove_user)
        self.parse_button.clicked.connect(self.start_parsing)
        self.generate_csv_button.clicked.connect(self.generate_csv)

        self.parser_thread = None
        self.update_statistics()  # Обновляем статистику при запуске

    def set_apple_style_theme(self):
        apple_style = """
        QWidget {
        background-color: #1C1C1E;
        color: #FFFFFF;
        font-family: 'San Francisco', Arial, sans-serif;
        font-size: 14px;
    }
    QLineEdit, QTextEdit, QListWidget, QComboBox {
        background-color: #2C2C2E;
        border: 1px solid #3A3A3C;
        border-radius: 8px;
        color: #FFFFFF;
        padding: 8px;
    }
    QLineEdit:focus, QTextEdit:focus, QListWidget:focus, QComboBox:focus {
        border: 1px solid #8E8E93;  /* Приглушённый акцент для фокуса */
    }
    QPushButton {
        background-color: #636366;  /* Приглушённый серый цвет */
        border: none;
        border-radius: 8px;
        color: #F2F2F7;
        padding: 10px 15px;
    }
    QPushButton:hover {
        background-color: #48484A;  /* Темнее при наведении */
    }
    QPushButton:pressed {
        background-color: #3A3A3C;  /* Ещё темнее при нажатии */
    }
    QProgressBar {
        background-color: #3A3A3C;
        border: none;
        border-radius: 8px;
        height: 15px;
        color: #FFFFFF;
        text-align: center;
    }
    QProgressBar::chunk {
        background-color: #8E8E93;  /* Приглушённый акцентный цвет */
        border-radius: 8px;
    }
    QLabel {
        color: #F2F2F7;
        font-weight: 500;
    }
    """
        self.setStyleSheet(apple_style)

    def create_tables(self):
        with self.conn:
            self.conn.execute('''CREATE TABLE IF NOT EXISTS group_ids (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    group_id TEXT UNIQUE)''')
            self.conn.execute('''CREATE TABLE IF NOT EXISTS user_ids (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    user_id TEXT UNIQUE)''')
            self.conn.execute('''CREATE TABLE IF NOT EXISTS users (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    vk_id TEXT UNIQUE,
                                    sex INTEGER,
                                    status TEXT)''')

    def load_group_ids(self):
        self.group_list.clear()
        with self.conn:
            group_ids = self.conn.execute("SELECT group_id FROM group_ids").fetchall()
            for group_id in group_ids:
                self.group_list.addItem(group_id[0])

    def load_user_ids(self):
        self.user_list.clear()
        with self.conn:
            user_ids = self.conn.execute("SELECT user_id FROM user_ids").fetchall()
            for user_id in user_ids:
                self.user_list.addItem(user_id[0])

    def add_group(self):
        group_id, ok = self.get_input_dialog("Введите Group ID:")
        if ok:
            self.group_list.addItem(group_id)
            with self.conn:
                self.conn.execute("INSERT OR IGNORE INTO group_ids (group_id) VALUES (?)", (group_id,))

    def remove_group(self):
        selected_items = self.group_list.selectedItems()
        if selected_items:
            for item in selected_items:
                group_id = item.text()
                self.group_list.takeItem(self.group_list.row(item))
                with self.conn:
                    self.conn.execute("DELETE FROM group_ids WHERE group_id = ?", (group_id,))

    def add_user(self):
        user_id, ok = self.get_input_dialog("Введите User ID:")
        if ok:
            self.user_list.addItem(user_id)
            with self.conn:
                self.conn.execute("INSERT OR IGNORE INTO user_ids (user_id) VALUES (?)", (user_id,))

    def remove_user(self):
        selected_items = self.user_list.selectedItems()
        if selected_items:
            for item in selected_items:
                user_id = item.text()
                self.user_list.takeItem(self.user_list.row(item))
                with self.conn:
                    self.conn.execute("DELETE FROM user_ids WHERE user_id = ?", (user_id,))

    def get_input_dialog(self, message):
        from PyQt5.QtWidgets import QInputDialog
        return QInputDialog.getText(self, "Input", message)

    def start_parsing(self):
        group_ids = [self.group_list.item(i).text() for i in range(self.group_list.count())]
        user_ids = [self.user_list.item(i).text() for i in range(self.user_list.count())]
        token = self.token_input.text()

        if not token:
            QMessageBox.warning(self, "Ошибка", "Пожалуйста, введите VK Token.")
            return

        self.parser_thread = VKParserWorker(group_ids, user_ids, token)
        self.parser_thread.update_progress.connect(self.progress_bar.setValue)
        self.parser_thread.log_message.connect(self.log_area.append)
        self.parser_thread.parsing_complete.connect(self.on_parsing_complete)
        self.parser_thread.start()

    def on_parsing_complete(self):
        self.log_area.append("Парсинг завершён!")
        self.update_statistics()  # Обновляем статистику после завершения парсинга

    def generate_csv(self):
        csv_type = self.csv_type_select.currentText()
        conn = sqlite3.connect(resource_path('vk_data.db'))
        query = "SELECT vk_id FROM users WHERE 1=1"

        if csv_type == "Мужчины":
            query += " AND sex=2"
        elif csv_type == "Женщины":
            query += " AND sex=1"
        elif csv_type == "Новые пользователи":
            query += " AND status='new'"
        elif csv_type == "Новые мужчины":
            query += " AND sex=2 AND status='new'"
        elif csv_type == "Новые женщины":
            query += " AND sex=1 AND status='new'"

        users = pd.read_sql_query(query, conn)

        if len(users) < 100:
            QMessageBox.warning(self, "Ошибка", "Список должен содержать не менее 100 уникальных записей.")
            conn.close()
            return

        # Добавляем необходимые столбцы для CSV, включая переименование vk_id в vk
        users = users.rename(columns={'vk_id': 'vk'})  # Переименование столбца vk_id в vk
        users['phone'] = ""
        users['email'] = ""
        users['ok'] = ""
        users['vid'] = ""
        users['gaid'] = ""
        users['idfa'] = ""

        # Заголовки столбцов для CSV
        columns = ['phone', 'email', 'ok', 'vk', 'vid', 'gaid', 'idfa']
        filename = f"vk_audience_{csv_type.replace(' ', '_').lower()}.csv"

        # Сохраняем данные в CSV
        users[columns].to_csv(
            filename,
            index=False,
            sep=',',
            encoding='utf-8',
            lineterminator='\n'
        )

        conn.close()

        QMessageBox.information(self, "CSV генерация", f"Генерация {csv_type} завершена! Файл: {filename}")

    def update_statistics(self):
        conn = sqlite3.connect(resource_path('vk_data.db'))
        total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        male_users = conn.execute("SELECT COUNT(*) FROM users WHERE sex=2").fetchone()[0]
        female_users = conn.execute("SELECT COUNT(*) FROM users WHERE sex=1").fetchone()[0]
        new_users = conn.execute("SELECT COUNT(*) FROM users WHERE status='new'").fetchone()[0]
        new_male_users = conn.execute("SELECT COUNT(*) FROM users WHERE sex=2 AND status='new'").fetchone()[0]
        new_female_users = conn.execute("SELECT COUNT(*) FROM users WHERE sex=1 AND status='new'").fetchone()[0]

        stats_text = (f"Всего пользователей: {total_users}\n"
                      f"Мужчин: {male_users}\n"
                      f"Женщин: {female_users}\n"
                      f"Новые пользователи: {new_users}\n"
                      f"Новые мужчины: {new_male_users}\n"
                      f"Новые женщины: {new_female_users}")

        self.stats_label.setText(stats_text)
        conn.close()


# Запуск приложения
if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
