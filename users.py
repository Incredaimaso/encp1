import json
from typing import List

class UserManager:
    def __init__(self, users_file: str = 'approved_users.json'):
        self.users_file = users_file
        self.approved_users: List[int] = self._load_users()

    def _load_users(self) -> List[int]:
        try:
            with open(self.users_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return []

    def _save_users(self):
        with open(self.users_file, 'w') as f:
            json.dump(self.approved_users, f)

    def add_user(self, user_id: int) -> bool:
        if user_id not in self.approved_users:
            self.approved_users.append(user_id)
            self._save_users()
            return True
        return False

    def is_approved(self, user_id: int, owner_id: int) -> bool:
        return user_id == owner_id or user_id in self.approved_users
