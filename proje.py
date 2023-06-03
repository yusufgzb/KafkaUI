import subprocess
from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates

app = FastAPI()
templates = Jinja2Templates(directory="templates")
consumer = None

@app.get("/")
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/")
def run_command(request: Request, operation: str = Form(..., alias="operation"), topic_name: str = Form(..., alias="topic_name")):
    global consumer

    bash_command = ""

    if operation == "Create":
        bash_command = f"sudo kafka_2.12-3.3.1/bin/kafka-topics.sh --create --topic {topic_name} --bootstrap-server localhost:9092"
    elif operation == "Delete":
        bash_command = f"sudo kafka_2.12-3.3.1/bin/kafka-topics.sh --delete --topic {topic_name} --bootstrap-server localhost:9092"
    elif operation == "List":
        bash_command = "sudo kafka_2.12-3.3.1/bin/kafka-topics.sh --list --bootstrap-server localhost:9092"
    elif operation == "Listen":
        bash_command = f"sudo kafka_2.12-3.3.1/bin/kafka-console-consumer.sh --topic {topic_name} --bootstrap-server localhost:9092 --partition 0 --offset $(kafka_2.12-3.3.1/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic {topic_name} --time -1 | awk -F ':' '{{sum+=$3}} END {{print sum-10}}') --max-messages 10"

    process = subprocess.Popen(bash_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    output = stdout.decode("utf-8") if stdout else stderr.decode("utf-8")
    return templates.TemplateResponse("index.html", {"request": request, "operation": operation, "topic_name": topic_name, "output": output})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)

