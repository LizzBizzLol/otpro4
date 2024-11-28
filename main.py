import argparse
import requests
import json
import os
from datetime import datetime


def get_vk_user_info(user_id=1, access_token=None):
    version = '5.199'  # last version of VK API (october 2024)

    url = f'https://api.vk.com/method/users.get?user_ids={user_id}&fields=followers_count,subscriptions&access_token={access_token}&v={version}'

    subscriptions_url = f'https://api.vk.com/method/users.getSubscriptions?user_id={user_id}&access_token={access_token}&v={version}'
    
    response = requests.get(url)
    subscriptions_response = requests.get(subscriptions_url)
    
    if response.status_code != 200:
        print("VK API error: ", response.text)
        return None
    
    data = response.json()
    
    if 'response' not in data:
        print("User Info data error: ", data)
        return None
    
    user_info = data['response'][0]
    
    if subscriptions_response.status_code != 200:
        print("Subscriptions error: ", subscriptions_response.text)
        return None
    
    subscriptions_data = subscriptions_response.json()
    
    user_info['groups'] = subscriptions_data.get('response', {}).get('groups', [])
    user_info['users'] = subscriptions_data.get('response', {}).get('users', [])
    return user_info
    
    # Запись данных в Neo4j
    with driver.session() as session:
        session.write_transaction(create_user_node, user_info['id'], user_info.get('screen_name', ''), 
                                  f"{user_info['first_name']} {user_info['last_name']}", user_info['sex'], 
                                  user_info.get('home_town', user_info.get('city', {}).get('title', '')))

        for group in user_info['groups']:
            session.write_transaction(create_group_node, group['id'], group['name'], group['screen_name'])
        
        for follower in user_info['users']:
            session.write_transaction(create_follow_relationship, follower['id'], user_info['id'])


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

def get_followers_and_subscriptions(user_id, access_token, depth=2):
    if depth == 0:
        return []
    
    user_info = get_vk_user_info(user_id, access_token)
    followers = user_info.get('followers', [])
    subscriptions = user_info.get('users', [])
    
    result = [{'user_id': user_id, 'info': user_info}]
    
    for follower_id in followers:
        result += get_followers_and_subscriptions(follower_id, access_token, depth - 1)
    
    for subscription_id in subscriptions:
        result += get_followers_and_subscriptions(subscription_id, access_token, depth - 1)
    
    return result

def create_group_node(tx, group_id, name, screen_name):
    query = """
    MERGE (g:Group {id: $group_id})
    SET g.name = $name,
        g.screen_name = $screen_name
    """
    tx.run(query, group_id=group_id, name=name, screen_name=screen_name)

def create_follow_relationship(tx, user1_id, user2_id):
    query = """
    MATCH (u1:User {id: $user1_id}), (u2:User {id: $user2_id})
    MERGE (u1)-[:FOLLOWS]->(u2)
    """
    tx.run(query, user1_id=user1_id, user2_id=user2_id)


def main():
    args = parse_arguments()
    user_info = get_vk_user_info(args.user_id, args.access_token)
    save_to_json(user_info, args.output_path)

from neo4j_utils import Neo4jHelper

neo4j = Neo4jHelper("bolt://localhost:7687", "neo4j", "adminadmin")

# Пример добавления пользователя и связей
neo4j.add_user(user_id=1, screen_name="user1", name="John Doe", sex=2, home_town="New York")
neo4j.add_relationship(1, 2, "FOLLOW")
neo4j.close()

from neo4j import GraphDatabase

# Функция для подключения к базе данных Neo4j
def connect_to_neo4j(uri, user, password):
    driver = GraphDatabase.driver(uri, auth=(user, password))
    return driver

# Пример использования
def create_user_node(tx, user_id, screen_name, name, sex, home_town):
    query = """
    MERGE (u:User {id: $user_id})
    SET u.screen_name = $screen_name,
        u.name = $name,
        u.sex = $sex,
        u.home_town = $home_town
    """
    tx.run(query, user_id=user_id, screen_name=screen_name, name=name, sex=sex, home_town=home_town)

# Создание подключения
uri = "bolt://localhost:7687"  # стандартный URI для Neo4j
user = "neo4j"
password = "adminadmin"  # замени на свой пароль
driver = connect_to_neo4j(uri, user, password)

# Пример записи данных
with driver.session() as session:
    session.write_transaction(create_user_node, 1, "screen_name", "Имя Фамилия", 1, "Город")

if __name__ == "__main__":
    main()
