import argparse
import json
import os

from googletrans import Translator
import mashina_mysql_v2

# initialize
current_path = os.path.dirname(os.path.abspath(__file__))
output = {}

def detect_language(text: str) -> str:
    translator = Translator()
    detected = translator.detect(text)
    return detected.lang

def translate_text(text: str, source_language: str, destination_language: str) -> str:
    if source_language == destination_language:
        return text

    translator = Translator()
    translated = translator.translate(text, src=source_language, dest=destination_language)
    return translated.text

def create_log_file() -> None:
    # Create log file if not exists
    if not os.path.exists(os.path.join(current_path, 'log.txt')):
        with open(os.path.join(current_path, 'log.txt'), 'w', encoding='utf-8') as f:
            f.write(f"Log file\n")
            f.write("------------------------------------------\n")

def get_ads(project: str) -> list:
    """
    Get all ads from the database based on the project name
    """
    mashina_mysql_v2.connect(project)

    if project == 'mashina':
        sql = '''
            SELECT ad.id, ad.slug, car.description, ad.updated_at
            FROM ad
            LEFT JOIN car ON ad.car_id = car.id
            WHERE ad.status = 1
        '''
    elif project in ['house', 'bazar']:
        columns = 'id, slug, description' if project == 'house' else 'id, description'
        sql = f'''
            SELECT {columns}, updated_at
            FROM ad
            WHERE status = 1
        '''
    else:
        raise ValueError(f"Unknown project: {project}")

    ads_df = mashina_mysql_v2.run_df(sql)
    mashina_mysql_v2.disconnect()

    if ads_df.shape[0] == 0:
        raise Exception('No ads found in the database')

    return ads_df.to_dict(orient='records')


def get_ad_by_slug(project: str, slug: str) -> dict:
    """
    Get a single ad from the database by slug based on the project name
    """
    mashina_mysql_v2.connect(project)

    if project == 'mashina':
        sql = f'''
            SELECT ad.id, ad.slug, car.description
            FROM ad
            LEFT JOIN car ON ad.car_id = car.id
            WHERE ad.slug = "{slug}" AND ad.status = 1
        '''
    elif project in ['house', 'bazar']:
        filter_column = 'slug' if project == 'house' else 'id'
        sql = f'''
            SELECT id, slug, description
            FROM ad
            WHERE {filter_column} = "{slug}" AND status = 1
        '''
    else:
        raise ValueError(f"Unknown project: {project}")

    ad_df = mashina_mysql_v2.run_df(sql)
    mashina_mysql_v2.disconnect()

    if ad_df.shape[0] == 0:
        raise Exception(f'No ad found with slug: {slug}')

    return ad_df.to_dict(orient='records')[0]


def update_or_insert_ad_translations(project: str, ad_id: int, translations: dict) -> None:
    """
    Update or insert ad translations in the database
    """
    mashina_mysql_v2.connect(project)

    # check if ad_id exists in ad_description
    check_sql = f'SELECT COUNT(*) AS count FROM ad_description WHERE ad_id = {ad_id}'
    check_df = mashina_mysql_v2.run_df(check_sql)

    if check_df.iloc[0]['count'] > 0:
        update_sql = f'''
            UPDATE ad_description 
            SET ru = "{translations['ru']}", 
                ky = "{translations['ky']}", 
                en = "{translations['en']}" 
            WHERE ad_id = {ad_id}
        '''
        mashina_mysql_v2.run(update_sql)
    else:
        insert_sql = f'''
            INSERT INTO ad_description (ad_id, ru, ky, en) 
            VALUES ({ad_id}, "{translations['ru']}", "{translations['ky']}", "{translations['en']}")
        '''
        mashina_mysql_v2.run(insert_sql)

    mashina_mysql_v2.disconnect()

def process_ad(project: str, ad: dict) -> None:
    if not ad.get('description'):
        # skip ads with empty descriptions
        return

    try:
        source_language = detect_language(ad['description'])
        # check and translate to the other languages
        translations = {
            'ru': translate_text(ad['description'], source_language, 'ru') if source_language != 'ru' else ad['description'],
            'ky': translate_text(ad['description'], source_language, 'ky') if source_language != 'ky' else ad['description'],
            'en': translate_text(ad['description'], source_language, 'en') if source_language != 'en' else ad['description'],
        }
        update_or_insert_ad_translations(project, ad['id'], translations)
    except Exception as e:
        with open(os.path.join(current_path, 'log.txt'), 'a', encoding='utf-8') as log_file:
            log_file.write(f"Error: {str(e)} for ad_id: {ad['id']}\n")

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--project', help='Project', required=True)
    parser.add_argument('-a', '--action', help='Action', required=True, choices=['translate_all', 'translate'])
    parser.add_argument('-s', '--slug', help='Slug', required=False)
    args = vars(parser.parse_args())

    create_log_file()

    try:
        project = args['project']
        action = args['action']

        if action == 'translate_all':
            ads = get_ads(project)
            for ad in ads:
                process_ad(project, ad)
        elif action == 'translate' and args.get('slug'):
            ad = get_ad_by_slug(project, args['slug'])
            process_ad(project, ad)
        else:
            raise ValueError('Slug must be provided for the translate action')

    except Exception as e:
        with open(os.path.join(current_path, 'log.txt'), 'a', encoding='utf-8') as log_file:
            log_file.write(f"Error: {str(e)}\n")

if __name__ == "__main__":
    main()
