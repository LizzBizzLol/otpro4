import argparse
import requests
import json
import os
from datetime import datetime
from collections import deque
from neo4j_utils import Neo4jHelper
import logging
import time

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

# Ограничение скорости запросов для VK API
def safe_request(url):
    time.sleep(0.33)  # Ограничение VK API — не более 3 запросов/сек
    try:
        response = requests.get(url).json()
        if 'error' in response:
            logging.error(f"VK API error: {response['error']['error_msg']}")
            return None
        return response.get('response', {})
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        return None

# Получение данных пользователя VK
def get_vk_user_info(user_id, access_token):
    version = '5.131'
    base_url = 'https://api.vk.com/method'

    user_url = f"{base_url}/users.get?user_ids={user_id}&fields=followers_count,city,sex,home_town&access_token={access_token}&v={version}"
    subscriptions_url = f"{base_url}/users.getSubscriptions?user_id={user_id}&access_token={access_token}&v={version}"
    followers_url = f"{base_url}/users.getFollowers?user_id={user_id}&access_token={access_token}&v={version}"

    user_data = safe_request(user_url)
    subscriptions = safe_request(subscriptions_url)
    followers = safe_request(followers_url)

    if not user_data or len(user_data) == 0:
        logging.error(f"Failed to fetch data for user_id: {user_id}")
        return None

    user_info = user_data[0]
    user_info['groups'] = subscriptions.get('items', []) if subscriptions else []
    user_info['followers'] = followers.get('items', []) if followers else []
    return user_info

# Сохранение данных в JSON
def save_to_json(user_info, output_path=None):
    if output_path is None:
        output_path = os.path.join(os.getcwd(), 'apiData')
    
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    
    filename = f"{user_info['first_name']}_{user_info['last_name']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    filepath = os.path.join(output_path, filename)
    
    with open(filepath, 'w', encoding='utf-8') as json_file:
        json.dump(user_info, json_file, ensure_ascii=False, indent=4)
    
    logging.info(f"Created and saved in {filepath}")

# Рекурсивное получение данных пользователей
def fetch_deep_data(user_id, access_token, neo4j, depth=2):
    queue = deque([(user_id, 0)])  # Очередь (user_id, current_depth)
    visited_users = set()

    while queue:
        current_user, current_depth = queue.popleft()
        if current_user in visited_users or current_depth > depth:
            continue

        visited_users.add(current_user)
        user_info = get_vk_user_info(current_user, access_token)
        if not user_info:
            continue

        neo4j.add_user(
            user_id=user_info['id'],
            screen_name=user_info.get('screen_name', ''),
            name=f"{user_info.get('first_name', 'Unknown')} {user_info.get('last_name', 'Unknown')}",
            sex=user_info.get('sex', None),
            home_town=user_info.get('home_town', '')
        )

        # Добавление подписок
        for group_id in user_info['groups']:
            neo4j.add_group(group_id)
            neo4j.add_relationship(user_info['id'], group_id, "SUBSCRIBED_TO")

        # Добавление подписчиков в очередь
        for follower_id in user_info.get('followers', []):
            neo4j.add_relationship(follower_id, user_info['id'], "FOLLOWED_BY")
            queue.append((follower_id, current_depth + 1))

# Запросы к Neo4j
def execute_queries(neo4j):
    queries = {
        "all_users": "MATCH (u:User) RETURN u",
        "top_5_users_by_followers": """
            MATCH (u:User)<-[:FOLLOWED_BY]-(f:User)
            RETURN u.id AS user_id, count(f) AS followers_count
            ORDER BY followers_count DESC
            LIMIT 5
        """,
        "top_5_popular_groups": """
            MATCH (g:Group)<-[:SUBSCRIBED_TO]-(u:User)
            RETURN g.id AS group_id, count(u) AS subscribers_count
            ORDER BY subscribers_count DESC
            LIMIT 5
        """
    }

    for query_name, query in queries.items():
        logging.info(f"Executing query: {query_name}")
        with neo4j.driver.session() as session:
            result = session.run(query)
            for record in result:
                logging.info(record)

# Разбор аргументов командной строки
def parse_arguments():
    parser = argparse.ArgumentParser(description='Fetch VK user data and process it.')
    parser.add_argument('user_id', help='VK User ID')
    parser.add_argument('access_token', help='Access Token for VK API')
    parser.add_argument('output_path', nargs='?', help='Output path for JSON file (default: current directory)')
    return parser.parse_args()

def main():
    args = parse_arguments()

    # Подключение к Neo4j
    neo4j = Neo4jHelper("bolt://localhost:7687", "neo4j", "adminadmin")
    try:
        # Создание индекса для оптимизации
        neo4j.create_index("User", "id")
        neo4j.create_index("Group", "id")

        # Получение данных из VK
        logging.info(f"Fetching data for user_id: {args.user_id}")
        user_info = get_vk_user_info(args.user_id, args.access_token)
        if not user_info:
            return
        
        # Сохранение данных в JSON
        save_to_json(user_info, args.output_path)

        # Добавление данных в Neo4j
        neo4j.add_user(
            user_id=user_info['id'],
            screen_name=user_info.get('screen_name', ''),
            name=f"{user_info.get('first_name', 'Unknown')} {user_info.get('last_name', 'Unknown')}",
            sex=user_info.get('sex', None),
            home_town=user_info.get('home_town', '')
        )

        # Обработка подписок
        for group_id in user_info['groups']:
            neo4j.add_group(group_id)
            neo4j.add_relationship(user_info['id'], group_id, "SUBSCRIBED_TO")

        # Обработка подписчиков второго уровня
        fetch_deep_data(args.user_id, args.access_token, neo4j, depth=2)
    finally:
        neo4j.close()

if __name__ == "__main__":
    main()
