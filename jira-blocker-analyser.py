import numpy as np
import re
from jira1 import JIRA
from datetime import datetime, timedelta
from bisect import bisect_right
import csv
import pandas as pd

#                МЕНЯТЬ ЛОГИН И ПАРОЛЬ ТУТ
# --------------------------------------------------------
user = Z_ENV_KAZARMENKOVA_LOG  # ЛОГИН
password = Z_ENV_KAZARMENKOVA_PW  # ПАРОЛЬ
# --------------------------------------------------------

# Тянем переменные из шапки
jira_server = $jira_server
project = $project
date = $date
category_pattern = $category_pattern
mode = $mode

# Функция поиска установки флага
def process_issue(jira, issue):
    changelog = issue.changelog
    
    flag_set_time = None
    flag_removed_time = None
    status_change_times = []
    comments = issue.fields.comment.comments
    blocker_infos = []

    for history in changelog.histories:
        history_created_time = datetime.strptime(history.created, '%Y-%m-%dT%H:%M:%S.%f%z')
        for item in history.items:
            if item.field == 'status':
                status_change_times.append(history_created_time)
            if item.field == 'Flagged':
                if item.toString == 'Impediment':
                    flag_set_time = history_created_time
                elif item.fromString == 'Impediment':
                    flag_removed_time = history_created_time

                # Если установлены и время установки, и время снятия флага, выводим информацию о блокировке и сбрасываем переменные
                if flag_set_time and flag_removed_time:
                    blocker_info = blocker_info_to_dict(issue, flag_set_time, flag_removed_time, comments, 'false')
                    blocker_infos.append(blocker_info)
                    flag_set_time = None
                    flag_removed_time = None
                # Если установлено только время флага, ищем ближайшую смену статуса
                if flag_set_time and not flag_removed_time:
                    status_change_times.sort()
                    index = bisect_right(status_change_times, flag_set_time)
                    if index != len(status_change_times):
                        blocker_info = blocker_info_to_dict(issue, flag_set_time, status_change_times[index], comments, 'true')
                        blocker_infos.append(blocker_info)
    return blocker_infos  
# Функция добавления в таблицу информации о блоке
def blocker_info_to_dict(issue, flag_set_time, flag_removed_time, comments, flag_was_not_removed):
    info_dict = dict()
    info_dict['Issue_Key'] = issue.key
    info_dict['Issue_Summary'] = issue.fields.summary
    info_dict['Flag_Set_Time'] = flag_set_time.strftime('%Y-%m-%d %H:%M')
    info_dict['Flag_Removed_Time'] = flag_removed_time.strftime('%Y-%m-%d %H:%M')

    time_flagged = flag_removed_time - flag_set_time
    info_dict['Time_Blocked'] = np.round(time_flagged.total_seconds() / (24*60*60), 1)

    info_dict['Blocker_Category'] = blocker_category_from_comment(comments, flag_set_time)
    info_dict['Comments'] = comments_text(comments, flag_set_time, flag_removed_time)

    info_dict['Flag_was_not_removed'] = flag_was_not_removed
    return info_dict
# функция достаем категорию блокера
def blocker_category_from_comment(comments, flag_set_time):
    for comment in comments:
#        category_pattern = r"#\w+"  # слово, начинающееся с #
#        category_pattern =  r'\{(.+?)\} # текст в фигурных скобках, {blocker+category}
        comment_time = datetime.strptime(comment.created, '%Y-%m-%dT%H:%M:%S.%f%z')
        if (comment_time - flag_set_time).total_seconds() <= 5 and (comment_time - flag_set_time).total_seconds() >= -5:
            match_r = re.search(category_pattern, comment.body)
            if match_r:
                return match_r.group(0)
    return ""
# Функция поиска комента блокировки
def comments_text(comments, flag_set_time, flag_removed_time):
    text = ""
    for comment in comments:
        comment_time = datetime.strptime(comment.created, '%Y-%m-%dT%H:%M:%S.%f%z')
        if flag_set_time <= comment_time <= flag_removed_time:
            text += comment.body + '\n'
    return text

# Тело скрипта

jira = JIRA(server=jira_server, basic_auth=(user, password))
    
issues = []
startAt = 0
maxResults = 50
  
while True:
    JQL = None
    if $JQL:
        JQL = '(comment ~ "(flag) Flag added" or comment ~ "(flag) Флажок добавлен") and ' + $JQL
    else:
        JQL = f'project = {project} and resolutiondate >= {date} and (comment ~ "(flag) Flag added" or comment ~ "(flag) Флажок добавлен")'
    chunk = jira.search_issues(JQL,
                            startAt=startAt,
                            maxResults=maxResults,
                            expand='changelog',
                            fields='comment,summary')
    if len(chunk) == 0:
        break
    issues.extend(chunk)
    startAt += maxResults

all_blocker_info = []

for issue in issues:
    blocker_infos = process_issue(jira, issue)
    all_blocker_info.extend(blocker_infos)  # use extend instead of append to add each dictionary separately
if not all_blocker_info:
    print("Не нашли задач с блокировками")
else:
    if mode == 'Текст':
        print(f">>>>> Found {len(issues)} issues <<<<<\n\n")
        for blocker_info in all_blocker_info:
            print(f"\n>>> Issue: {blocker_info['Issue Key']} - {blocker_info['Issue Summary']} <<<\n")
            print(f"Block set:     {blocker_info['Flag Set Time']}")
            print(f"Block removed: {blocker_info['Flag Removed Time']}\n")
            if blocker_info['Flag was not removed']:
                print("Flag was not removed!!! First status change after flag set considered as blocker removed\n")
            print(f"Time blocked (days): {blocker_info['Time Blocked']}\n=======\n")
            if blocker_info['Blocker Category']:
                print(f"Blocker category: {blocker_info['Blocker Category']}\n______")
            print(f"Comment: \n{blocker_info['Comments']}\n")
    elif mode == 'Таблица':
        all_blocker = pd.DataFrame(all_blocker_info)
        tf.df_to_gp(all_blocker, 'all_blocker_info', gp_service = 'gp')
