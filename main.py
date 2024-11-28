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
    user_info = get_vk_user_info(args.user_id, args.access_token)
    save_to_json(user_info, args.output_path)


if __name__ == "__main__":
    main()
