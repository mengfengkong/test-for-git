# coding:utf-8
import pymysql
import os
import pandas as pd
from mixpanel_utils import MixpanelUtils
import time
import json
import datetime
from sqlalchemy import create_engine

import logging

logging.getLogger().setLevel(logging.INFO)


def getFileList(fileDir: str):
    """获取指定路径下所有json文件的绝对路径

    Args:
        fileDir (str): 目录名称

    Returns:
        数组: 目录下所有json文件的绝对路径字符串数组
    """
    ret = []
    path = os.path.abspath(fileDir)
    if os.path.exists(path):
        files = os.listdir(path)
        for f in files:
            if os.path.splitext(f)[1] == '.json':
                ret.append(path + '/' + f)  # 获取文件的绝对路径
    else:
        logging.error('{}路径不存在'.format(path))
    return ret


def get_key(jsonObj, keystr):
    """判断json/dict中是否存在某个key

    Args:
        keystr (string): key名称字符串

    Returns:
        string: 返回key对应的json值
    """
    # obj是字典的情况
    if isinstance(jsonObj, dict):
        for k, v in jsonObj.items():
            if k == keystr:
                return v
            else:
                ret = get_key(v, keystr)
                if ret:
                    return ret
    # obj是数组的情况
    elif isinstance(jsonObj, list):
        for x in jsonObj:
            ret = get_key(x, keystr)
            if ret:
                return ret
    else:
        return None


def merge_key(jsonObj, keys: list):
    """提取部分jsonkey，合并为新的json

    Args:
        jsonObj (str): json字符串
        keys (list): 需要合并的json键列表

    Returns:
        [str]: [将key值数组合并为一个json串进行返回]]
    """
    ret = {}
    if len(keys) == 0:
        return ""
    for item in keys:
        ret[item] = get_key(jsonObj, item)
    return json.dumps(ret)


def jsonToDF(filePath: str, eventDict: dict):
    """解析JSON文本文件，按照我们的要求转化为DataFrame数据结构

    Args:
        filePath (str): 文件路径
        eventDict (dict): event事件对象，用来设计DataFrame数据结构

    Returns:
        DataFrame: 加工后的DataFrame
    """
    if not os.path.exists(filePath):
        logging.error('{}文件不存在'.format(filePath))
        return

    f = open(filePath, 'r')
    json_obj = json.loads(f.read())

    res = []
    for item in json_obj:
        # 根据事件名称，从event字典中得到用户properties列表
        event_name = item['event']
        user_props = eventDict.get(event_name)

        properties = item['properties']

        obj = {}
        time_num = get_key(properties, 'mp_processing_time_ms')  # 20220209-直接用time会有日期不是当天的情况
        obj['time'] = time_num

        # 将时间戳格式的记录时间转化为可读的数据格式
        timeArray = time.localtime(time_num / 1000)
        otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
        obj['record_time'] = otherStyleTime

        # mixpanel公有属性
        obj['event_name'] = item['event']
        obj['distinct_id'] = get_key(properties, 'distinct_id')
        obj['browser'] = get_key(properties, '$browser')
        obj['browser_version'] = get_key(properties, '$browser_version')
        obj['city'] = get_key(properties, '$city')
        obj['current_url'] = get_key(properties, '$current_url')
        obj['device_id'] = get_key(properties, '$device_id')
        obj['distinct_id_before'] = get_key(properties, '$distinct_id_before')
        obj['initial_reffer'] = get_key(properties, '$initial_reffer')
        obj['initial_reffer_domain'] = get_key(properties, '$initial_reffer_domain')
        obj['insert_id'] = get_key(properties, '$insert_id')
        obj['lib_version'] = get_key(properties, '$lib_version')
        obj['mp_api_endpoint'] = get_key(properties, '$mp_api_endpoint')
        obj['os'] = get_key(properties, '$os')
        obj['reffer'] = get_key(properties, '$reffer')
        obj['reffer_domain'] = get_key(properties, '$reffer_domain')
        obj['region'] = get_key(properties, '$region')
        obj['screen_height'] = get_key(properties, '$screen_height')
        obj['screen_width'] = get_key(properties, '$screen_width')
        obj['user_id'] = get_key(properties, '$user_id')
        obj['mp_country_code'] = get_key(properties, 'mp_country_code')
        obj['mp_lib'] = get_key(properties, 'mp_lib')
        obj['mp_processing_time_ms'] = get_key(properties, 'mp_processing_time_ms')

        # 埋点的自定义属性字段
        obj['user_properties'] = merge_key(properties, user_props)

        # 数据加工日期
        obj['etl_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        res.append(obj)

    f.close()
    logging.info('DataFrame数据构建完毕')
    return pd.DataFrame(res)


def json2db(file_path: str, eventDict: dict):
    """将DataFrame通过pandas的to_sql方法写入数据库

    Args:
        file_path (str): 文件路径
    """
    # 基础类实例
    engine = create_engine(
        "mysql+pymysql://treelab_bi:w9Jiua6T6AGhsgkm@172.17.0.25:4000/dwb_mixpanel?charset=utf8")  # 生产库
    # engine = create_engine("mysql+pymysql://root:henan715@localhost:3306/mixpanel?charset=utf8")  # 本地测试数据库
    conn = engine.connect()

    # 构建DataFrame
    df = jsonToDF(file_path, eventDict)
    if not df.empty:
        # 写数据库
        df.to_sql(name='mixpanel_data_web', con=conn, index=False, if_exists='append')
        logging.info('文件{}写入完毕~'.format(file_path.split('/')[-1]))
    else:
        logging.info('DataFrame结果为空，已跳过')


def checkDataHasDownloaded(dirPath: str, fileAbsPath: str):
    """判断下载文件夹内是否已经有指定日期的数据，用于后续跳过已经有数据的日期，这里理论上从数据库获取会更准确，
       考虑到mixpanel接口的导出量限制，将所有导出的文件都本地存放了一份副本以防万一。

    Args:
        dirPath (str): 数据导出文件存放的路径
        fileAbsPath (str): 具体文件的绝对路径

    Returns:
        布尔: True-已存在，False-不存在
    """
    saved_file_list = getFileList(dirPath)

    if fileAbsPath in saved_file_list:
        return True
    else:
        return False


def downloadData(eventDict, startDate):
    """调用mixpanel utils，从服务器下载数据，以JSON格式保存到本地

    Args:
        eventDict (dict): event字典
        startDate (str): yyyy-mm-dd格式的、需要下载的数据日期（默认只下载一天的数据）
    """
    # 构建下载文件存放目录
    saveDirPath = './events'
    if not os.path.exists(saveDirPath):
        os.mkdir(saveDirPath)

    # 以日期为保存文件的名称
    saveFileName = '{}/{}.json'.format(os.path.abspath(saveDirPath), startDate.replace('-', ''))

    # 如果文件已经下载，则直接跳过
    if checkDataHasDownloaded(saveDirPath, saveFileName):
        logging.info('{}日数据重复，已跳过~'.format(startDate))
        return

    # 构建mixpanel工具导出数据
    eventList = list(eventDict.keys())

    try:
        # 调用官方工具导出数据（两个日期相同，既下载一天的数据，设置不同的起始日期可以下载多天的数据）

        mutils = MixpanelUtils('f29bce3c6c8cda7888ebc00570704bc1', '6966560a2ca3be7509748990a8bd377d')
        mutils.export_events(output_file='{}'.format(saveFileName),
                             format='json',
                             params={
                                 'from_date': startDate,
                                 'to_date': startDate,
                                 'event': eventList
                             }
                             )
        logging.info('{}数据导出完毕，开始写入数据库……'.format(startDate))

        # 写入数据库
        json2db(file_path=saveFileName, eventDict=eventDict)
        logging.info('{}数据写入完毕~'.format(startDate))

    except Exception as e:
        logging.info('导出异常，日期为{}，异常信息：{}'.format(startDate, e))


def run():
    run_date = datetime.date.today()  # 脚本运行的时间
    download_date = (run_date + datetime.timedelta(-1)).strftime('%Y-%m-%d')  # 下载昨天的数据

    print(download_date)

    # event事件清单（需要定期手动更新）
    events_dict = {
        # "$ae_first_open" : ["device_type", "version"],
        # "$ae_session" : ["device_type", "version"],
        # "$ae_updated" : ["device_type", "version"],
        # "Block Coped / Moved" : ["Click Button", "Is Duplicate"],
        # "Block Created" : ["APP Name", "Entity ID", "Form Where", "Workspace Id"],
        # "Click Video Title V2" : ["Title"],
        # "click_different_workspace" : ["device_type", "type", "version", "workspace_id"],
        # "click_mark_all_read" : ["device_type", "type", "version"],
        # "click_notification_item" : ["device_type", "notification_id", "notification_type", "type", "version"],
        # "click_record_detail_follow" : ["device_type", "is_follow", "row_id", "version"],
        # "click_record_detail_share" : ["device_type", "row_id", "version"],
        # "click_sign_next_button" : ["device_type", "login_type", "type", "version"],
        # "click_tabbar_button" : ["device_type", "tabbar_type", "type", "version"],
        # "click_task_list_filter" : ["device_type", "type", "version"],
        # "Column Created V1.1" : ["Entry Point"],
        # "Column Updated V1.1" : ["Column Properties", "Column Type"],
        # "Core Operations" : ["Number of inputs","Operation type","Backend error","Column type","Final number of columns","Is column type change","Is new column","Values","View type","Number of rows removed","New #values","Previous values","Entity type"],
        # "create column" : ["Column type","Table id","View id","View type","Workspace id"],
        # "create row" : ["Table id", "View id", "Workspace id"],
        # "create table" : ["Workspace id"],
        # "create view" : ["Table id", "View id", "View type", "Workspace id"],
        # "CreateWorkspaceByTemplate V2" : ["scene", "templateName"],
        # "Download Attachment" : ["Column ID", "File ID","File Name","File Type","Row ID","Table ID","Workspace ID","Worksplace ID"],
        # "Entity Created V1.1" : ["Entity Type", "Entry Point", "View Type", "Depth"],
        # "Entity Description" : ["Entity type"],
        # "Entity Updated V1.1" : ["Date", "Entity Type", "Entry Point"],
        # "Export Excel" : ["File Name","Table ID","View ID","Workspace ID","Worksplace ID"],
        # "Folder Page Photo V1.1" : ["Folder Page Photo - Is Uploaded Photo"],
        # "Formula Action" : ["Formula"],
        # "Get Free Template HomePage V2" : ["Title"],
        # "Get Template V2" : ["Templates", "Title"],
        # "Global Add Row" : ["View Type"],
        # "Global Event" : ["Backend error","Operation name","Operation variables","Frontend error"],
        # "Import Excel":["Action","File Format"],
        "ImportExcel": ["duration", "fileSize", "fileType", "success", "url"],
        "Invitation Accepted V1.1": ["Received data from getInvitedToWorkspace query"],
        "Login Sign Up Action V1.1": ["Action", "Auth Type", "Email", "Phone Number"],
        "Login V1.1": ["Auth Type", "Channel", "Login Step", "Treelab User ID"],
        "Logout": ["Time spent in session until logout"],
        "Onboarding V1.1": ["Action", "Section Index"],
        "open_app": ["device_type", "from", "open_type", "type", "version"],
        "Page View V1.1": ["Path", "Route", "WorkspaceId"],
        "Paste From Clipboard": ["dataCameFromExternalSource", "newColumnCount", "newRowCount",
                                 "optimisticUpdateTimeMs", "pastedCellCount", "pastedColumnCount", "pastedRowCount",
                                 "targetViewId"],
        "Play Video V2": ["Duration", "Title"],
        "Questionnaire V1.1": ["Is First Time Creating Workspace?", "Is Skipped?", "Is Workspace Avatar Changed?",
                               "Is Workspace Name Changed", "Response - Industry", "Response - Team Size",
                               "Telephone Number", "Workspace ID", "Workspace Name", "Workspace Type",
                               "Is Workspace Name Changed?", "Email"],
        "remove column": ["Table id", "View id", "Workspace id"],
        "remove row": ["Table id", "View id", "Workspace id"],
        "remove table": ["Workspace id"],
        "remove view": ["Table id", "View id", "Workspace id"],
        "Row Created V1.1": ["Entry Point", "View Type"],
        "Send Invitation Link Action V1.1": ["Link Permission", "To Where"],
        "Send Invitation V1.1": ["# Email", "# Existing Guests", "# New Guests", "# Phone", "# Workspace Members",
                                 "Permission", "To Where"],
        "Session": ["Time spent in app in seconds"],
        "Share Link Action V1.1": ["Content"],
        "Sign Up V1.1": ["Auth Type", "Channel", "Is With Extra Template?"],
        "sign_in_success": ["device_type", "login_type", "type", "version"],
        "sign_up_success": ["device_type", "login_type", "type", "version"],
        "Stay Time V2": ["User Guide Time Stamp"],
        "Switch Video V2": ["Title"],
        "task_list_filter_success": ["device_type", "filter_type", "type", "version"],
        "Template Action V1.1": ["Action", "Is User Logged In or Signed In", "Template ID", "User ID"],
        "Template Import V1.1": ["Channel", "Is New Workspace?", "Template ID", "Template Name"],
        "Template Visit V1.1": ["Template ID", "Template Name"],
        "use_app_duration": ["device_type", "duration", "type", "version"],
        "User Guide Click Upload V2": ["num"],
        "User Guide Next V2": ["num"],
        "User Guide Skip V2": ["skipNumber"],
        "View Control Operations": ["Is global control", "Operation type", "Value", "View type", "Column type"],
        "view_record_detail": ["device_type", "type", "version"],
        "view_record_detail_sub_page": ["device_type", "sub_page", "type", "version"],
        "view_sign_in_sign_up_page": ["device_type", "type", "version"],
        "view_task_detail": ["device_type", "type", "version"],
        "view_task_detail_sub_page": ["device_type", "sub_page", "type", "version"],
        "Visit Public Invitation Link V1.1": ["Inviter Id", "Log In Status", "Permission", "To Where"],
        "Visit Share Link V1.1": ["Content", "Share Id", "Source"],
        "Visit Share View Link V1.1": ["Share Id", "Source", "View Type"],
        # "$session_start":[],
        # "$session_end":[]
    }

    downloadData(events_dict, download_date)


if __name__ == '__main__':
    run()
