import requests
import time
import pandas as pd


def get_all_group_members(group_id, token):
    members = []
    url = "https://api.vk.com/method/groups.getMembers"
    count = 1000  # Максимальное количество участников за один запрос
    offset = 0
    while True:
        params = {
            "group_id": group_id,
            "fields": "id, deactivated, sex",
            "access_token": token,
            "v": "5.131",
            "count": count,
            "offset": offset
        }
        response = requests.get(url, params=params)
        data = response.json()

        if 'error' in data:
            print(f"Ошибка: {data['error']['error_msg']}")
            break

        members_batch = data["response"]["items"]
        members.extend(members_batch)

        if len(members_batch) < count:
            break

        offset += count
        time.sleep(0.34)

    return members


def get_user_friends(user_id, token):
    url = "https://api.vk.com/method/friends.get"
    params = {
        "user_id": user_id,
        "access_token": token,
        "v": "5.131",
        "fields": "sex, deactivated",
    }
    response = requests.get(url, params=params)
    data = response.json()

    if 'error' in data:
        print(f"Ошибка при получении друзей пользователя {user_id}: {data['error']['error_msg']}")
        return []

    return data["response"]["items"]


def get_user_followers(user_id, token):
    followers = []
    url = "https://api.vk.com/method/users.getFollowers"
    count = 1000  # Максимальное количество подписчиков за один запрос
    offset = 0
    while True:
        params = {
            "user_id": user_id,
            "access_token": token,
            "v": "5.131",
            "count": count,
            "offset": offset,
            "fields": "sex, deactivated"
        }
        response = requests.get(url, params=params)
        data = response.json()

        if 'error' in data:
            print(f"Ошибка при получении подписчиков пользователя {user_id}: {data['error']['error_msg']}")
            break

        followers_batch = data["response"]["items"]
        followers.extend(followers_batch)

        if len(followers_batch) < count:
            break

        offset += count
        time.sleep(0.34)

    return followers


# Массив с ID групп и ID пользователей
group_ids = ["172840335", "201664431", "81156844", "217275804", "211965477", "213150907", "211293719"]
user_ids = ["374049794", "365737792", "120054533", "195171988"]
token = "c05fb139c05fb139c05fb139eec37f8041cc05fc05fb139a75a3debccd91f262294bb60"

male_members = []
female_members = []

# Получаем подписчиков групп
for group_id in group_ids:
    try:
        members = get_all_group_members(group_id, token)
        # Оставляем только активных пользователей
        active_members = [member for member in members if 'deactivated' not in member]

        # Разделяем по полу
        for member in active_members:
            sex = member.get('sex', 0)  # Если пол не указан, используем 0
            if sex == 1:
                female_members.append(member['id'])
            elif sex == 2:
                male_members.append(member['id'])

        print(f"Группа {group_id}: получено {len(active_members)} активных подписчиков")

    except Exception as e:
        print(f"Ошибка при обработке группы {group_id}: {e}")

# Получаем друзей и подписчиков из профилей пользователей
for user_id in user_ids:
    try:
        # Получаем друзей
        friends = get_user_friends(user_id, token)
        # Оставляем только активных друзей
        active_friends = [friend for friend in friends if 'deactivated' not in friend]

        for friend in active_friends:
            sex = friend.get('sex', 0)  # Если пол не указан, используем 0
            if sex == 1:
                female_members.append(friend['id'])
            elif sex == 2:
                male_members.append(friend['id'])

        print(f"Профиль {user_id}: получено {len(active_friends)} активных друзей")

        # Получаем подписчиков
        followers = get_user_followers(user_id, token)
        # Оставляем только активных подписчиков
        active_followers = [follower for follower in followers if 'deactivated' not in follower]

        for follower in active_followers:
            sex = follower.get('sex', 0)  # Если пол не указан, используем 0
            if sex == 1:
                female_members.append(follower['id'])
            elif sex == 2:
                male_members.append(follower['id'])

        print(f"Профиль {user_id}: получено {len(active_followers)} активных подписчиков")

    except Exception as e:
        print(f"Ошибка при обработке пользователя {user_id}: {e}")

# Убираем дубликаты
male_members = list(set(male_members))
female_members = list(set(female_members))

# Сохраняем мужчин в файл
if len(male_members) > 0:
    male_data = [{"phone": "", "email": "", "ok": "", "vk": f"{user_id}", "vid": "", "gaid": "", "idfa": ""} for user_id in male_members]
    male_df = pd.DataFrame(male_data)
    # Используем lineterminator для переноса строки
    male_df.to_csv("vk_ad_male_audience.csv", index=False, sep=',', encoding='utf-8', lineterminator='\n')
    print(f"Всего уникальных мужчин: {len(male_members)}. Список сохранен в vk_ad_male_audience.csv")

# Сохраняем женщин в файл
if len(female_members) > 0:
    female_data = [{"phone": "", "email": "", "ok": "", "vk": f"{user_id}", "vid": "", "gaid": "", "idfa": ""} for user_id in female_members]
    female_df = pd.DataFrame(female_data)
    # Используем lineterminator для переноса строки
    female_df.to_csv("vk_ad_female_audience.csv", index=False, sep=',', encoding='utf-8', lineterminator='\n')
    print(f"Всего уникальных женщин: {len(female_members)}. Список сохранен в vk_ad_female_audience.csv")

