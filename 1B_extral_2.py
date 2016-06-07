# -*- coding: utf-8 -*-
'''
新章节3.5前：不等式和三角形
统计免费／收费 各知识点
    进入视频 -> 开始播放视频 -> (退出视频) / 完成视频 
    -> 进入练习(PV & UV) -> (完成一道题) -> 退出练习 / 练习通过 / 练习失败 (PV & UV)
    计算每一步的PV & UV
@author: wangguojie
上线后每周一次, 累积
'''
from bson.objectid import ObjectId
from pymongo import MongoClient
from use import *
from collections import OrderedDict
import datetime

chapters = MongoClient('10.8.8.111:27017')['onions30']['chapters']
topics = MongoClient('10.8.8.111:27017')['onions30']['topics']
events = MongoClient('10.8.8.111:27017')['koalaBackupOnline']['events']

# START_DATE = ONLINE_DATE
END_DATE = datetime.datetime(2016,05,03,16)
START_DATE = END_DATE - datetime.timedelta(weeks=4)


steps = OrderedDict([
    ("enterHyperVideo", "进入视频"), 
    ("startVideo", "开始视频"),
    ("quitVideo", "退出视频"),
    ("finishVideo", "完成视频"),
    ("startMaster", "进入练习模块"),
    ("submitAnswer", "完成一道题"),
    ("quitProblem", "退出题目"), #即退出练习
    ("problemSetFailure", "专题失败"), #专题失败＋挑战失败＝练习失败
    ("challengeFailure", "挑战失败"),
    ("completeMaster", "完成练习模块") #即练习通过
])


chapter_ids = [ObjectId("538fe05c76cb8a0068b14031"), ObjectId("54f3cd322964c9cf2002c81a")]
topic_name = {}

def processing():
    topic_ids = unpack(aggregate(chapters,
                                     Pipeline().match(_id={"$in": map(ObjectId, chapter_ids)})
                                     .group(_id=None, topics={"$addToSet": "$topics"}).get(),
                                     "topics")[0])
    agg_list = list(topics.aggregate(Pipeline().match(_id={"$in": topic_ids}).project(topic="$_id", name="$name", _id=0).get()))
    for topic_info in agg_list:
        topic_name.update({str(topic_info['topic']): str(topic_info['name']).decode('utf-8')})
    
    print("Topic总数: " + str(len(topic_ids)))
    
    result_list = []
    for topic_id in topic_ids:
        video_ids = unpack(aggregate(topics,
                                   Pipeline().match(_id=ObjectId(topic_id))
                                   .group(_id=None, videos={"$addToSet": "$learning.hyperVideos"}).get(),
                                    "videos")[0])
        for video_id in video_ids:
            user_type = "device"        
            result_list.append(sub_processing(str(video_id), user_type))
    print result_list

def sub_processing(video_id, user_type):
    practice_stats = {}
    
    video_ids = video_id
    
    problem_ids = unpack(aggregate(topics,
                                   Pipeline().match(_id=video_id)
                                   .group(_id=None,problems={"$addToSet": "$master.problemSets"}).get(),
                                             "problems")[0])
    video_ids = map(str, video_ids)
    problem_ids = map(str, problem_ids)
    parent_list_pc = []
    parent_list_mobile = []

    for node in steps:
        if node == "enterHyperVideo":
            parent_list_pc = aggregate(events,
                                       Pipeline().match(platform="web", eventKey=node, eventValue__videoId={"$in":video_ids},
                                                         serverTime={"$gte": START_DATE, "$lt": END_DATE})\
                .group(_id=None, users={"$push": "$user"}).get(), "users")[0]
            parent_list_mobile = aggregate(events,
                                            Pipeline().match(platform='app', eventKey=node, eventValue__videoId = {"$in":video_ids},
                                                                    serverTime = {"$gte":START_DATE,"$lt":END_DATE})
                                                        .group(_id = None, users = {"$push":"$user"}).replace("user",user_type),
                                            "users".replace("user", user_type))[0]

            practice_stats.update({node:[len(parent_list_pc+parent_list_mobile), len(set(parent_list_pc + parent_list_mobile))]})

        if node == "startVideo" or node == "finishVideo":
            parent_list_pc = aggregate(events,
                                       Pipeline().match(platform="web", eventKey=node, eventValue__videoId={"$in": video_ids},
                                                        user={"$in": parent_list_pc},
                                                         serverTime={"$gte": START_DATE, "$lt": END_DATE})\
                .group(_id=None, users={"$push": "$user"}).get(), "users")[0]

            parent_list_mobile = aggregate(events,
                            Pipeline().match(platform='app', eventKey = node, eventValue__videoId = {"$in":video_ids},user = {"$in":parent_list_mobile},
                                                    serverTime = {"$gte":START_DATE,"$lt":END_DATE})
                                        .group(_id = None, users = {"$push":"$user"}).replace("user",user_type),
                            "users".replace("user",user_type))[0]
            practice_stats.update({node:[len(parent_list_pc+parent_list_mobile), len(set(parent_list_pc + parent_list_mobile))]})

        if node == "quitVideo":
            result_list_pc = aggregate(events,
                                       Pipeline().match(platform="web", eventKey=node, eventValue__videoId={"$in": video_ids},
                                                        user={"$in": parent_list_pc},
                                                         serverTime={"$gte": START_DATE, "$lt": END_DATE})\
                .group(_id=None, users={"$push": "$user"}).get(), "users")[0]

            result_list_mobile = aggregate(events,
                                            Pipeline().match(platform='app',eventKey = node, eventValue__videoId = {"$in":video_ids}, user = {"$in":parent_list_mobile},
                                                                serverTime = {"$gte":START_DATE,"$lt":END_DATE})
                                                        .group(_id = None, users = {"$push":"$user"}).replace("user",user_type),
                                            "users".replace("user",user_type))[0]
            practice_stats.update({node:[len(result_list_pc+result_list_mobile), len(set(result_list_pc+result_list_mobile))]})

        if node == "startMaster":
            parent_list_pc = aggregate(events,
                                       Pipeline().match(platform="web", eventKey=node, eventValue__topicId=topic_id,
                                                        user={"$in": parent_list_pc},
                                                         serverTime={"$gte": START_DATE, "$lt": END_DATE})\
                .group(_id=None, users={"$push": "$user"}).get(), "users")[0]

            parent_list_mobile = aggregate(events,
                                        Pipeline().match(platform='app',eventKey = node, eventValue__topicId = topic_id,user = {"$in":parent_list_mobile},
                                                            serverTime = {"$gte":START_DATE,"$lt":END_DATE})
                                                    .group(_id = None,users = {"$push":"$user"}).replace("user",user_type),
                                        "users".replace("user",user_type))[0]
            practice_stats.update({node:[len(parent_list_pc+parent_list_mobile), len(set(parent_list_pc + parent_list_mobile))]})

        if node == "submitAnswer":
            result_list_pc = aggregate(events,
                                       Pipeline().match(platform="web", eventKey=node, eventValue__problemId={"$in": problem_ids},
                                                        user={"$in": parent_list_pc},
                                                         serverTime={"$gte": START_DATE, "$lt": END_DATE})\
                .group(_id=None, users={"$push": "$user"}).get(), "users")[0]

            result_list_mobile = aggregate(events,
                                                Pipeline().match(platform='app',eventKey = node, eventValue__problemId = {"$in":problem_ids},user = {"$in":parent_list_mobile},
                                                                    serverTime = {"$gte":START_DATE,"$lt":END_DATE})
                                                            .group(_id = None, users = {"$push":"$user"}).replace("user",user_type),
                            "users".replace("user",user_type))[0]
            practice_stats.update({node:[len(result_list_pc+result_list_mobile), len(set(result_list_pc+result_list_mobile))]})

        if node == "quitProblem":
            result_list_pc = aggregate(events,
                                       Pipeline().match(platform="web", eventKey=node, eventValue__problemId={"$in": problem_ids},
                                                        user={"$in": parent_list_pc},
                                                         serverTime={"$gte": START_DATE, "$lt": END_DATE})\
                .group(_id=None, users={"$push": "$user"}).get(), "users")[0]

            result_list_mobile = aggregate(events,
                                                Pipeline().match(platform='app',eventKey = node, eventValue__problemId = {"$in":problem_ids},user = {"$in":parent_list_mobile},
                                                                    serverTime = {"$gte":START_DATE,"$lt":END_DATE})
                                                            .group(_id = None, users = {"$push":"$user"}).replace("user",user_type),
                                                "users".replace("user",user_type))[0]
            practice_stats.update({node:[len(result_list_pc+result_list_mobile), len(set(result_list_pc+result_list_mobile))]})

        if node == "problemSetFailure" or "challengeFailure":
            result_list_pc = aggregate(events,
                                       Pipeline().match(platform="web", eventKey=node, eventValue__topicId=topic_id,
                                                        user={"$in": parent_list_pc},
                                                         serverTime={"$gte": START_DATE, "$lt": END_DATE})\
                .group(_id=None, users={"$push": "$user"}).get(), "users")[0]

            result_list_mobile = aggregate(events,
                                                Pipeline().match(platform='app',eventKey = node, eventValue__topicId = topic_id,user = {"$in":parent_list_mobile},
                                                                    serverTime = {"$gte":START_DATE,"$lt":END_DATE})
                                                            .group(_id = None, users = {"$push":"$user"}).replace("user",user_type),
                                                "users".replace("user",user_type))[0]
            practice_stats.update({node:[len(result_list_pc+result_list_mobile), len(set(result_list_pc+result_list_mobile))]})

        if node == "completeMaster":

            practices_state = ["perfect", "imperfect"]

            result_list_pc = aggregate(finishState,
                                       Pipeline().match(user={"$in": map(str, parent_list_pc)}, topic=topic_id, finishState={"$in": practices_state},
                                                         serverTime={"$gte": START_DATE, "$lt": END_DATE})\
                .group(_id=None, users={"$push": "$user"}).get(), "users")[0]

            if user_type is 'user':
                result_list_mobile = aggregate(finishState,
                                       Pipeline().match(user={"$in": map(str, parent_list_mobile)}, topic=topic_id,finishState={"$in": practices_state},
                                                         serverTime={"$gte": START_DATE, "$lt": END_DATE})\
                .group(_id=None, users={"$push": "$user"}).get(), "users")[0]

            else:
                result_list_mobile = aggregate(events,
                                                Pipeline().match(platform='app',eventKey = 'enterTopicFinish', eventValue__topicId = topic_id,user = {"$in":parent_list_mobile},
                                                                    serverTime = {"$gte":START_DATE,"$lt":END_DATE})
                                                            .group(_id = None, users = {"$push":"$user"}).replace("user",user_type),
                                                "users".replace("user",user_type))[0]

            practice_stats.update({node:[len(result_list_pc+result_list_mobile), len(set(result_list_pc+result_list_mobile))]})
        print practice_stats            
    print video_id
    print practice_stats
    return video_id, practice_stats


# run this script like this:
#      $> python    **.py   result.txt [ start_date end_date ]
# result.txt <-- stored result
if __name__ == '__main__':
    try:
        print("start data statistics and analysis ......")
        processing()
        print("data processing finished.")

    except:
        print("data processing error...... -->" + __file__)
        raise



