# -*- coding: utf-8 -*-
"""
Created on Mon Mar 30 23:19:46 2020

@author: Omar Salah https://github.com/romasalah/
"""
import os
import re
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import numpy
from googletrans import Translator
import pdb
import sys
import click
import time
from pathlib import Path
import subprocess
import traceback
import signal

def tweet_accum(inputtweets,limit=13500,separator=' --- '):
    oneorgtweet_i=0
    alltweetbatches=[]
    allaccumids=[]
    while oneorgtweet_i<len(inputtweets)-1:
        tempstring=''
        accumidtemp=''
        for oneorgtweet_i in range(oneorgtweet_i,len(inputtweets)):
            curid=inputtweets.iloc[oneorgtweet_i]['tweet_id']
            if len(tempstring)>limit:
                alltweetbatches.append(tempstring)
                allaccumids.append(accumidtemp)
                break
            else:
                redtweet=re.sub(r'(http.*(\s|$)|\@.*(\s|$))', ' ',inputtweets.iloc[oneorgtweet_i]['original_tweet'],flags=re.MULTILINE)
                redtwid=curid
                if len(redtweet)<10:
                    redtweet='Leer'
                    redtwid='0000'
                tempstring=tempstring+separator+redtweet
                accumidtemp=accumidtemp+separator+redtwid
    accumulateddict={'accumulated_tweets': alltweetbatches, 'accumulated_ids': allaccumids}
    accumulated=pd.DataFrame(data=accumulateddict)
    return accumulated
            
def scan_directory(datasetdir):
    orgtweetfilesdir=[]
    orgtweetsfull=pd.DataFrame()
    for root, _, files in os.walk(datasetdir):
       for name in files:
           orgtweetfilesdir.append(os.path.join(root, name))
           df_table = pq.read_table(os.path.join(root, name))
           tw_table=df_table.to_pandas()
           orgtweetsfull=orgtweetsfull.append(tw_table,ignore_index=True)
           orgtweetsfull=orgtweetsfull.drop_duplicates(keep='last')
           pddict={ 'tweet_id': orgtweetsfull['id'], 'original_tweet': orgtweetsfull['text']}
    orgtweets=pd.DataFrame(data=pddict)
    orgtweets=orgtweets.drop_duplicates(keep='last')
    return orgtweets,orgtweetsfull
          
def divide_tweets(accumulatedtweets,separator,prefix):
    dividedtweets=[]
    alldivs=[]
    for curtweetbatch in accumulatedtweets:
        splitbatch=curtweetbatch.split(separator)
        subbatch=[]
        for isub in splitbatch:
            tmp=re.sub(r'§§§', '', isub, flags=re.MULTILINE)
            subbatch.append(tmp.strip())
        dividedtweets.extend(subbatch)
        alldivs.append(splitbatch)
    return dividedtweets,alldivs


CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}
IPINFO_API_TOKEN = "7ea87c6b8a2d76"
VPNS = list(Path(".", "vpns").glob("vpn-*.premiumize.me.ovpn"))

def translate_vpn(inputtweets,rotate=True,ignore_errors=1):
    pos_current_vpn = 0

    # Helper variable to abort after one exception if `rotate` is not `True`.
    rotating = True
    startpos=0
    badtweets_i=[]
    translatedtweets=[]
    listuntranslated=[]
    listuntranslated_i=[]
    listuntranslatedtweetids=[]
    translatedtweetids=[]
    translatedtweetids=[]
    fullsz=len(inputtweets)

    # try:
    while rotating:
            # current_vpn = VPNS[pos_current_vpn].as_posix()

            # # Start the VPN in another process.
            # click.echo(f"[INFO] Start openvpn process with '{current_vpn}'.")
            
            # if sys.platform == "win32":
            #     process = subprocess.Popen(["openvpn", current_vpn], shell=True,)
            # else:
            #     process = subprocess.Popen(
            #         ["sudo", "openvpn", current_vpn], shell=True, preexec_fn=os.setsid,
            #     )
            # # Safety measure to allow the VPN to connect.
            # click.echo("[INFO] Wait for openvpn to establish the VPN connection.")
            # time.sleep(20)
            # click.echo("[INFO] Start translating task.")
            if ignore_errors==1:
                print('now to translate')
                translatedtweets0,translatedtweetids0,startpos,untranslated,untranslated_i,untranslatedtweetid,failed=translate(inputtweets,0,startpos,batch=1,progress=1,ignore_errors=ignore_errors)
                translatedtweets.extend(translatedtweets0[0:startpos+1])
                translatedtweetids.extend(translatedtweetids0[0:startpos+1])
                listuntranslated.append(untranslated)
                listuntranslatedtweetids.append(untranslatedtweetid)
                listuntranslated_i.append(untranslated_i)
                #inputtweets=listuntranslated
                show_progress(fullsz-len(listuntranslated),fullsz)
                if startpos==len(inputtweets)-1:
                    rotating=False
                if failed!=0:
                    print('Stop Error')
                    badtweets_i.append(startpos)
                    startpos=failed+1
                    pdb.set_trace()
            else:
                translatedtweets0,translatedtweetids0,startpos,untranslated,untranslated_i,untranslatedtweetid0,failed=translate(inputtweets,0,startpos,batch=1,ignore_errors=ignore_errors)
                translatedtweets.append(translatedtweets0[0:startpos+1])
                translatedtweetids.extend(translatedtweetids0[0:startpos+1])
                listuntranslated.append(untranslated)
                listuntranslatedtweetids.append(untranslatedtweetid)
                listuntranslated_i.append(untranslated_i)
                show_progress(startpos,len(inputtweets))
                print(startpos)
                if startpos==len(inputtweets)-1:
                    rotating=False
                if failed==1:
                    print('Stop Error')
                    input('Change the VPN and Press Enter...')
                    badtweets_i.append(startpos)
                    startpos=startpos+1
                # click.secho(
                #     "[WARNING] An exception occurred. Terminate the openvpn process.",
                #     color="yellow",
                # )
                # click.secho(f"\n{traceback.print_exc()}n", color="yellow")
                # if sys.platform == "win32":
                #     # Found here: https://stackoverflow.com/a/17614872.
                #     subprocess.Popen(f"TASKKILL /F /PID {process.pid} /T")
                # else:
                #     # Found here: https://stackoverflow.com/a/4791612.
                #     os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                # #Iterate through the VPNs and start again if the last VPN is reached.
                # pos_current_vpn = (pos_current_vpn + 1) % len(VPNS)
            if not rotate:
                rotating = False
    # except (KeyboardInterrupt, SystemExit):
    #     click.echo("Scraping is interrupted.")
    #     sys.exit(0)
    return translatedtweets,translatedtweetids,listuntranslated,listuntranslated_i,listuntranslatedtweetids


def translate(alltweetbatches,separateonerror=0,startpos=0,separator=' --- ',batch=1,progress=1,ignore_errors=1,maxerrors=5):
    tsclient=Translator()
    if batch==1:
        untranslated=[]
        untranslated_i=[]
        untranslatedtweetid=[]
        translatedtweets=['']*len(alltweetbatches)
        translatedtweetids=['']*len(alltweetbatches)
        failed=0
        for startpos in range(startpos,len(alltweetbatches)):
                print(startpos)
                try:
                    translatedtweets[startpos]=tsclient.translate(alltweetbatches.loc[startpos]['accumulated_tweets'],dest='en',src='de')
                    translatedtweetids[startpos]=alltweetbatches.loc[startpos]['accumulated_ids']
                    print('batch translated')
                except:
                    if separateonerror:
                         tempdivided=alltweetbatches.loc[startpos]['accumulated_tweets'][len(separator):]
                         splitbatch=tempdivided.split(separator)
                         untranslated.append(splitbatch)
                         untranslated_i.append(startpos)
                         #break
                        # divide the batch into single tweets
                         try:
                            translatedtweets[startpos]=tsclient.translate(splitbatch,dest='en',src='de')
                         except:
                            # collect untranslated tweets
                            failed=1
                            untranslated=alltweetbatches.loc[startpos]['accumulated_tweets']
                            untranslatedtweetid=alltweetbatches.loc[startpos]['accumulated_ids']
                            untranslated_i=startpos
                            translatedtweets[startpos]='fail'
                    else:
                        print('fail')
                        failed=1
                        untranslated.append(alltweetbatches.loc[startpos]['accumulated_tweets'])
                        untranslatedtweetid.append(alltweetbatches.loc[startpos]['accumulated_tweets'])
                        untranslated_i.append(startpos)
                        untranslateddiff=numpy.diff(untranslated_i)
                        oneloc=numpy.where(untranslateddiff == 1)
                        if len(oneloc[0]) > maxerrors:
                            failed=startpos-maxerrors
                            break
                    if ignore_errors==1:
                        pass
                    else:
                        break
                if progress==1:
                    show_progress(startpos,len(alltweetbatches))
    else:
        translatedtweets=[]
        for startpos in range(startpos,len(alltweetbatches)):
             try:
                 translatedtweets.append(tsclient.translate(alltweetbatches[startpos], dest='en', src='de'))
             except:
                 untranslated.extend(alltweetbatches[startpos])
                 pass
    return translatedtweets,translatedtweetids,startpos,untranslated,untranslated_i,untranslatedtweetid,failed

def unpack_trbatch(translatedbatchlist):
    twtext=['']*len(translatedbatchlist)
    for twtext_i in range(0,len(translatedbatchlist)):
        try:
            twtext[twtext_i]=translatedbatchlist[twtext_i].text
        except:
            pass
    return twtext

def show_progress(curpos,total):
    sys.stdout.write("\r{0}%".format(round(curpos/total*100,2)))
    sys.stdout.flush()
    time.sleep(0.5)
    
def find_orgtweets(ids,orgtweets):
    orgtweetsback=['']*len(ids)
    orgids=orgtweets['tweet_id'].tolist()
    orgtext=orgtweets['original_tweet'].tolist()
    for curid_i in range(0,len(ids)):
        try:
            curidloc=orgids.index(ids[curid_i])
            orgtweetsback[curid_i]=orgtext[curidloc]
        except:
            pass
    return orgtweetsback

def compare_translaions(tweets1,tweets2):
    tweets1=pd.read_csv(r'translated\API1.csv')
    tweets2=pd.read_excel(r'translated\API2.xlsx')
    newtweets1=[]
    newtweetids=[]
    newtweets2=[]
    originaltweets=[]
    for index,curtweet2 in tweets2.iterrows():
        #print(curtweet1.iloc['id'])
        tw2locintw1=tweets1['id'][tweets1['id']==curtweet2['tweet_id']].index
        if not tw2locintw1.empty:
            curtweet1=tweets1.iloc[tw2locintw1]
            newtweetids.append(tweets2.iloc[index]['tweet_id'])
            newtweets1.append(curtweet1.iloc[0]['translation'])
            newtweets2.append(tweets2.iloc[index]['Translated_tweets'])
            originaltweets.append(curtweet1.iloc[0]['original_text'])
    dfdict={'id': newtweetids,'original': originaltweets,'API1': newtweets1,'API2': newtweets2}
    twcomparison=pd.DataFrame(data=dfdict)
    twcomparison.to_excel(r'translated\API_comparison.xlsx')
    comp_table = pa.Table.from_pandas(twcomparison)
    pq.write_table(comp_table, r'translated\API_comparison.parquet')
    lasti=-1
    for samplei in range(1,7):
        sample=twcomparison.iloc[lasti+1:samplei*200,]
        sample.to_excel('translated\\Sample '+str(samplei)+'.xlsx') 
        lasti=samplei*200
        
    
if __name__ == "__main__":
    # import tweets
    datasetdir=r"D:\Professional\corona\twitter-translate"
    os.chdir(datasetdir)
    separator=' §§§CUT§§§ '
    redseparator='CUT'
    #pdb.set_trace()
    orgtweets,orgtweetsfull=scan_directory(datasetdir)
    # Accumulate tweets
    alltweetbatches=tweet_accum(orgtweets,1000,separator)
    #Translate accumulated tweets
    # translatedtweets,startpos,untranslated,untranslated_i=translate_vpn(alltweetbatches)
    translatedtweets,translatedtweetids,tuntranslated,untranslated_i,untranslatedtweetids=translate_vpn(alltweetbatches,ignore_errors=1)
    # get the results as text
    nonempt=[None]
    tmp=translatedtweets.index('')
    if type(tmp)==int:
        nonempt[0]=tmp
    else:
        nonempt=tmp
    translatedtweets = [x for i,x in enumerate(translatedtweets) if i not in nonempt]
    translatedtweetids = [x for i,x in enumerate(translatedtweetids) if i not in nonempt]
    twtext=unpack_trbatch(translatedtweets)
    tweetsingles,alltw=divide_tweets(twtext,redseparator,'§§§')
    tweetidssingles,allid=divide_tweets(translatedtweetids,redseparator,'§§§')
    tmp=tweetidssingles.any('')
    nonempt2='1' in tweetidssingles
    tweetidssingles2 = [x for x in enumerate(tweetidssingles) if '12' in x]
    orgtweetsback=find_orgtweets(tweetidssingles,orgtweets)
    orgtransiddict={'tweet_id':tweetidssingles,'original_tweet':tweetsingles,'translated_tweet': orgtweetsback}
    dfObj=pd.DataFrame(data=orgtransiddict)
    trtw_table = pa.Table.from_pandas(dfObj)
    dfObj.to_excel('Translated_tweets.xlsx')
    pq.write_table(trtw_table, 'Translated_tweets.parquet')
