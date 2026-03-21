import requests
import time

class Ntfy(object):
    def __init__(self, topic):
        self.topic = topic
        self.last_mesg_ts = 0

    def send_alert(self, message, title="cookie", priority="high", tags="warning,cookie"):
        url = f"https://ntfy.sh/{self.topic}"
        headers = {
            "Title": title,
            "Priority": priority, # "urgent", "high", "default", "low", "min"
            "Tags": tags
        }
    
        response = requests.post(url, data=message, headers=headers)
    
        if response.status_code == 200:
            self.last_mesg_ts = time.time()
            pass#print("Ntfy sent successfully!")
        else:
            print(f"Ntfy error: {response.status_code}")