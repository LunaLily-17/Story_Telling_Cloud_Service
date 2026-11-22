"""Smoke-check 202 Accepted jobs, concurrency and global state consistency."""
import json, os, time
from concurrent.futures import ThreadPoolExecutor
from urllib.request import Request, urlopen

BASE_URL = os.getenv('STORY_BASE_URL', 'http://127.0.0.1:8000').rstrip('/')

def call(path, data=None):
    payload = None if data is None else json.dumps(data).encode()
    with urlopen(Request(BASE_URL+path,payload,
                         {'Content-Type':'application/json'}),timeout=20) as response:
        return response.status,json.load(response)

def wait(job_id):
    for _ in range(90):
        _,job=call('/jobs/'+job_id)
        if job['status'] in ('completed','conflict','failed'): return job
        time.sleep(1)
    raise RuntimeError('Job did not finish')

if __name__=='__main__':
    _,created=call('/stories', {'title':'Hidden Map','genre':'mystery',
        'premise':'Alex searches for a hidden map in a flooded village.'})
    sid=created['story_id']
    def submit(i):return call(f'/stories/{sid}/continue',{'instruction':i,'expected_version':0})
    with ThreadPoolExecutor(max_workers=2) as pool:
        submissions=list(pool.map(submit,['Search tunnel carefully','Explore archive']))
    assert all(code==202 for code,_ in submissions),submissions
    finished=[wait(item['job_id']) for _,item in submissions]
    _,story=call('/stories/'+sid)
    assert story['version']==1 and len(story['chapters'])==1,story
    assert sorted(x['status'] for x in finished)==['completed','conflict'],finished
    print('PASS: one authoritative chapter, no lost writes, conflict detected')
    print('Browser:',f'{BASE_URL}/?story={sid}')
