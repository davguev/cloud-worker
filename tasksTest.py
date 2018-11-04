from celery import Celery
app = Celery()

import os
BROKER_URL_PARAM = os.environ.get("REDIS_URL", "redis://")
CELERY_RESULT_BACKEND_PARAM = os.environ.get("REDIS_URL", "redis://")
app.conf.update(BROKER_URL=BROKER_URL_PARAM, CELERY_RESULT_BACKEND=CELERY_RESULT_BACKEND_PARAM)

@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # Transformar archivos cada X segundos
    sender.add_periodic_task(20.0, task.s("hello"), name='add every 20')
    
@app.task
def task(arg):
    print("Hola")
