import argparse
import requests
import json
import os
from datetime import datetime
from neo4j_utils import Neo4jHelper

def get_vk_user_info(user_id=1, access_token=None):
    version = '5.199'

    url = f'https://api.vk.com/method/users.get?user_ids={user_id}&fields=followers_count&access_token={access_token}&v={version}'
    subscriptions_url = f'https://api.vk.com/method/users.getSubscriptions?user_id={user_id}&access_token={access_token}&v={version}'
    
    response = requests.get(url).json()
    subscriptions_response = requests.get(subscriptions_url).json()
    
    if 'error' in response:
        print(f"Error: {response['error']['error_msg']}")
        return None
    
    user_info = response['response'][0]
    
    # Обработка подписок (группы и пользователи)
    user_info['groups'] = subscriptions_response.get('response', {}).get('items', [])
    return user_info

def save_to_json(user_info, output_path=None):
    if output_path is None:
        output_path = os.path.join(os.getcwd(), 'apiData')
    
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    
    filename = f"{user_info['first_name']}_{user_info['last_name']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    filepath = os.path.join(output_path, filename)
    
    with open(filepath, 'w', encoding='utf-8') as json_file:
        json.dump(user_info, json_file, ensure_ascii=False, indent=4)
    
    print(f"Created and saved in {filepath}")

def parse_arguments():
    parser = argparse.ArgumentParser(description='Get VK user info and save it to a JSON file.')
    parser.add_argument('user_id', help='VK User ID')
    parser.add_argument('access_token', help='Access Token for VK API')
    parser.add_argument('output_path', nargs='?', help='Output path for the JSON file (default: current directory)')
    
    return parser.parse_args()

def main():
    args = parse_arguments()
    
    # Подключение к Neo4j через Neo4jHelper
    neo4j = Neo4jHelper("bolt://localhost:7687", "neo4j", "adminadmin")

    try:
        # Получение данных из VK
        user_info = get_vk_user_info(args.user_id, args.access_token)
        if not user_info:
            return
        
        # Сохранение данных в JSON
        save_to_json(user_info, args.output_path)

        # Добавление данных в Neo4j
        neo4j.add_user(
            user_id=user_info['id'],
            screen_name=user_info.get('screen_name', ''),
            name=f"{user_info['first_name']} {user_info['last_name']}",
            sex=user_info['sex'],
            home_town=user_info.get('home_town', '')
        )

        # Пример добавления связей
        for group in user_info['groups']:
            neo4j.add_user(group['id'], group.get('name', 'ввав'), 'фи', 'м', 'город')  # Добавь корректные поля
            neo4j.add_relationship(user_info['id'], group['id'], "SUBSCRIBED_TO")
    finally:
        neo4j.close()

if __name__ == "__main__":
    main()
