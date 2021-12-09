


from collections import UserList
from typing import Counter


class Instaspider():
    #imports
    import os
    import json
    import time
    import random
    import requests
    from datetime import datetime
    from bs4 import BeautifulSoup
    from kafka import KafkaProducer
    import urllib.parse as parser
    
    cookies=""
    userlist=[]
    kafk_bootstrap_servers=""
    kafka_topic=""
    producer = None
    output_path=""
    USER_AGENT = {}
    counter=0
    def __init__(self,cookies:str,userlist:list,kafk_bootstrap_servers:str,kafka_topic:str,output_path:str,USER_AGENT:str) -> None:
        self.USER_AGENT={"User-Agent":USER_AGENT}
        self.cookies=cookies
        self.userlist=userlist
        self.kafk_bootstrap_servers=kafk_bootstrap_servers
        self.kafka_topic=kafka_topic
        self.output_path=output_path
    def Request(self,url:str,params=None,method='get'):
        self.time.sleep(self.random.randint(0,2))
        if method == 'post':
            pass
        if method =='get':
            return (self.requests.get(url=url,cookies=self.cookies,params=params,headers=self.USER_AGENT))
    def start(self):
        self.KafkaProducer(bootstrap_servers=[self.kafk_bootstrap_servers],
                        value_serializer=lambda x: 
                        self.json.dumps(x).encode('utf-8'))
        #loop over username list
        for username in self.userlist:
            url = f'https://www.instagram.com/{username}/'
            res=self.Request(url=url)
            self.parse(response=res)
    #send data to kafka
    def send_to_kafka(self,input:dict):
        producer = self.KafkaProducer(value_serializer=lambda m: self.json.dumps(m).encode('ascii'))
        producer.send(self.kafka_topic, input)
    #---------------------------------- making path with username  ----------------------------------
    def make_path(self,name:str)->str:
        return (self.os.path.dirname(__file__)+self.output_path+name+".json")
    #---------------------------------- done function chek output status and if all three status ==true write output to the file ----------------------------------
    def done(self,output:dict):
        if output['userinfostatus'] and output['igtvstatus'] and output['postsstatus']:
            username=output['userinfo']['username']
            output.pop('userinfostatus')
            output.pop('igtvstatus')
            output.pop('postsstatus')
            path = self.make_path(username)
            file = open(path,"w")
            self.json.dump(output,file)
            file.close()
            self.counter+=1
            print("\n    done("+str(self.counter)+")    \n")
    #---------------------------------- parse taged people from list of them ----------------------------------
    def parse_tagged_people(people:list)->list:
        output =[]
        #chaecking if ther is any person tagged
        if len(people)==0:
            return output
        #loop over people
        for persion in people:
            user=persion['node']["user"]
            data={
                        "username": str(user["username"]),
                        "fullname": str(user["full_name"]),
                        "is_verified": str(user["is_verified"])
                    }
            output.append(data)
        return output   
    #---------------------------------- parse posts from list of edges ----------------------------------
    def parse_edges(self,edges:list,output:dict,key:str):
        result = list(output[key])
        #loop over edges
        for edge in edges:
            edge=edge['node']
            #extract type
            type={
                "from":"posts",
                "type":"image"
            }
            if edge['is_video']:
                type['type']="video"
            if key=="igtvposts":
                type['from']="igtv"
            #exract owner
            owner={}
            try:
                owner=edge['owner']
            except:
                owner={}
            #extact caption
            captions = ""
            try:
                if edge['edge_media_to_caption']:
                    if len(edge['edge_media_to_caption']['edges'])==1:
                        captions=edge['edge_media_to_caption']['edges'][0]['node']['text']
                    else:
                        for cap in edge['edge_media_to_caption']['edges']:
                            captions += cap['node']['text']
            except:
                captions=""
            #extract location
            location=""
            try:
                #igtv does not have location
                if key !="igtvposts":
                    if edge['location']:
                        location=str(edge['location'])
            except:
                location=""
            #extract instagram describe of post
            instagram_describe=""
            try:
                if edge['accessibility_caption']:
                    instagram_describe=str(edge['accessibility_caption']).replace(',',";")
            except:
                instagram_describe=""
            #extract tagged people
            tagged = []
            try:
                tagged= self.parse_tagged_people(edge['edge_media_to_tagged_user']['edges'])
            except:
                tagged=[]
            #create data
            data = {
            "owner":owner,
            "date_of_creation": str(self.datetime.fromtimestamp(edge['taken_at_timestamp']).strftime("%d/%m/%Y %H:%M:%S")),
            "type": type,
            "caption":captions,
            "number_of_like": str(edge['edge_media_preview_like']['count']),
            "number_of_comment":str(edge['edge_media_to_comment']['count']),
            "location": location,
            "tagged_people":tagged,
            "instagram_describe": instagram_describe
            }
            #append data to final output of function
            result.append(data)
            self.send_to_kafka(data)
        #adding all posts that function extract to final output
        d={key:result}
        output.update(d)
    #---------------------------------- if posts more than 12 then this function get all posts ----------------------------------
    def parse_posts(self, response,output:dict,di:dict):
            data = self.json.loads(response.text)
            self.parse_edges(data['data']['user']['edge_owner_to_timeline_media']['edges'],output,"posts")
            
            #checking if there is next page
            next_page_bool = data['data']['user']['edge_owner_to_timeline_media']['page_info']['has_next_page']
            if next_page_bool:
                #create requets to the instagram server to get next 12 posts
                cursor = data['data']['user']['edge_owner_to_timeline_media']['page_info']['end_cursor']
                di['after'] = cursor
                params = {'query_hash': '8c2a529969ee035a5063f2fc8602a0fd', 'variables': self.json.dumps(di)}
                url = 'https://www.instagram.com/graphql/query/?' + self.parser.urlencode(params)
                res=self.Request(url=url,params=di)
                self.parse_posts(response=res,output=output,di=di)
            else:
                #set post status true and call done function
                output['postsstatus']=True
                self.done(output)
    #---------------------------------- if igtv posts more than 12 then this function get all igtv posts ----------------------------------
    def parse_igtvposts(self, response,output:dict,di:dict):
            data = self.json.loads(response.text)
            self.parse_edges(data['data']['user']['edge_felix_video_timeline']['edges'],output,"igtvposts")
            #checking if there is next page
            next_page_bool = data['data']['user']['edge_felix_video_timeline']['page_info']['has_next_page']
            if next_page_bool:
                #create requets to the instagram server to get next 12 posts
                cursor = data['data']['user']['edge_felix_video_timeline']['page_info']['end_cursor']
                di['after'] = cursor
                params = {'query_hash': 'bc78b344a68ed16dd5d7f264681c4c76', 'variables': self.json.dumps(di)}
                url = 'https://www.instagram.com/graphql/query/?' + self.parser.urlencode(params)
                res=self.Request(url=url,params=di)
                self.parse_igtvposts(response=res,output=output,di=di)
            else:
                #set igtv status true and call done function
                output['igtvstatus']=True
                self.done(output)
    #---------------------------------- parse main pages ----------------------------------
    def parse(self, response): 

        #convert response to dict
        bs=self.BeautifulSoup(response.text,features="lxml")
        body = bs.find('body')
        script = body.find('script', text=lambda t: t.startswith('window._sharedData')).text
        json_string = script.strip().split(' = ')[1][:-1]
        input = self.json.loads(json_string)
        #extract user
        user=input["entry_data"]["ProfilePage"][0]["graphql"]["user"]
        #extract biography
        biography =""
        try:
            if user['biography']:
                biography=str(user['biography'])
        except:
            biography=""
        #create final output dict
        output ={
        "userinfostatus":True,
        "igtvstatus":False,
        "postsstatus":False,
        
        "userinfo": {
            "username": str(user['username']),
            "fullname": str(user['full_name']),
            "biography": biography,
            "is_verified": str(user['is_verified']),
            "number_of_igtv_posts":str(user['edge_felix_video_timeline']['count']),
            "number_of_posts":str(user['edge_owner_to_timeline_media']['count']),
            "number_of_followers":str(user['edge_followed_by']['count'] ),
            "number_of_folowing": str(user['edge_follow']['count'])
        },
        "igtvposts":[],
        "posts": []
        }
        #checking that user has igtv post
        if len(user['edge_felix_video_timeline']['edges'])!=0:
            self.parse_edges(user['edge_felix_video_timeline']['edges'],output,"igtvposts")
            #checking that user igtv posts has next page
            if user['edge_felix_video_timeline']['page_info']['has_next_page']:
                #creat request for instagram server to get next 12 igtv posts
                user_id=str(user['id'])
                cursor=user['edge_felix_video_timeline']['page_info']['end_cursor']
                di = {'id': user_id, 'first': 12, 'after': cursor}
                params = {'query_hash': 'bc78b344a68ed16dd5d7f264681c4c76', 'variables': self.json.dumps(di)}
                url = 'https://www.instagram.com/graphql/query/?' + self.parser.urlencode(params)
                #send request and send parse_igtvposts as callback func
                res=self.Request(url=url,params=di)
                self.parse_igtvposts(response=res,output=output,di=di)
            else:
                #set igtv status true and call done func
                output['igtvstatus']=True
                self.done(output)
        else:
            #set igtv status true and call done func
            output['igtvstatus']=True
            self.done(output)
        #checking that user has post
        if len(user['edge_owner_to_timeline_media']['edges'])!=0:
            self.parse_edges(user['edge_owner_to_timeline_media']['edges'],output,"posts")
            #checking that user posts has next page
            if user['edge_owner_to_timeline_media']['page_info']['has_next_page']:
                #creat request for instagram server to get next 12 posts
                user_id=str(user['id'])
                cursor=user['edge_owner_to_timeline_media']['page_info']['end_cursor']
                di = {'id': user_id, 'first': 12, 'after': cursor}
                params = {'query_hash': '8c2a529969ee035a5063f2fc8602a0fd', 'variables': self.json.dumps(di)}
                url = 'https://www.instagram.com/graphql/query/?' + self.parser.urlencode(params)
                #send request and send parse_pots as callback func
                res=self.Request(url=url,params=di)
                self.parse_posts(response=res,output=output,di=di)
            else:
                #set posts status true and call done func
                output['postsstatus']=True
                self.done(output)
        else:
            #set posts status true and call done func
            output['postsstatus']=True
            self.done(output)
        

    
