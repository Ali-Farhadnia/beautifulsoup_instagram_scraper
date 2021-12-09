import spider

cookies={
            'ig_nrcb':'1',
            'mid':'YWMiUgALAAEr8PEAL_BE1wvUih3K	',
            'ig_did':'99F6FF7E-C3B4-471F-86EB-3FDF54E7F858',
            'shbid':'"14492\05444040163983\0541665424736:01f7a4b3719f78c62d5bfaa8fb5b61af00e5c68a0dc5497fede250664ce1674aa89a11f5"',
            'shbts':'"1633888736\05444040163983\0541665424736:01f7aa4b36cc29546904d4550087536941c07ffb5843fca8df97b1eedc6471f0d4fc29a7"',
            'csrftoken':'21l7iyZ7dDUti3dGaAiWsH3rflpqB7FK',
            'ds_user_id':'44040163983',
            'sessionid':'44040163983%3AFFliM3E38UmH4u%3A4',
            'rur':'"VLL\05444040163983\0541665432387:01f78ed5e86655931724e3a76a0753c09a3592fae5cba6ac2e7a5a5bef8a625f569eee39"',    
        }
userlist=["davidderuiter","reqdyy","yeezy.visions",
"noah_gesser","noahhaynes88","b1essah","oliver.skipp",
"oliverbenoitt","oliverjburke","n.o_369_","elijahtatis",
"mxelijah","iamelijahfisher","pcdubss","williamyyellow",
"boldyjames","aguerobenja19","ben_white6","strombeck_","lucasgagliasso"]
output_path="/../bs_data/"
user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36'
myspider=spider.Instaspider(cookies=cookies,
                            userlist=userlist,
                            kafk_bootstrap_servers='localhost:9092',
                            kafka_topic='rawdata',
                            output_path=output_path,
                            USER_AGENT=user_agent
                            )

myspider.start()