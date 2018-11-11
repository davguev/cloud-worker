# -*- coding: utf-8 -*-

from celery import Celery
from celery.schedules import crontab
from pymongo import MongoClient
from bson import ObjectId
import boto3
import os
import ffmpy
import sendgrid
from sendgrid.helpers.mail import *
import datetime
import sys
import email.utils
from email.mime.text import MIMEText as text
from email.mime.multipart import MIMEMultipart as mp

app = Celery()

BROKER_URL_PARAM = os.environ.get("REDIS_URL", "redis://")
CELERY_RESULT_BACKEND_PARAM = os.environ.get("REDIS_URL", "redis://")
app.conf.update(BROKER_URL=BROKER_URL_PARAM, CELERY_RESULT_BACKEND=CELERY_RESULT_BACKEND_PARAM)

@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # Transformar archivos cada X segundos
    sender.add_periodic_task(30.0, test2.s("hello"), name='add every 5')

@app.task
def test2(arg):
    if os.environ["TRANSFORMANDO"] == "SI":
        return
    try:
        client = None
        s3res = boto3.resource('s3')
        s3cli = boto3.client('s3')
        res = boto3.resource("sqs")
        queue_url = os.environ["URL_SQS"]
        # Receive message from SQS queue
        queue = res.Queue(queue_url)
        messages = queue.receive_messages(MaxNumberOfMessages=1)
        if(len(messages) > 0):
            mensaje = messages[0]
            id = int(mensaje.body)
            os.environ["TRANSFORMANDO"] = "SI"
            path_videos = os.environ["PATH_VIDEOS"]
            client = MongoClient(os.environ["DB_HOSTNAME"] + ":" + os.environ["DB_PORT"],
                                 username=os.environ["DB_USER"],
                                 password=os.environ["DB_PASSWORD"],
                                 authSource=os.environ["DB_NAME"],
                                 authMechanism="SCRAM-SHA-1")
            db = client[os.environ["DB_NAME"]]
            videos = db.publications_publication
            concursos = db.contests_contest
            videoObj = videos.find_one({"id":id})
            if videoObj is None:
                print("No hay registros.")
            else:
                videos.update_one(
                    { "id": id },
                    { "$set": { "status": "E" } }
                )
                video = str(videoObj["video"])
                carpeta_archivo = video.split("/")
                carpeta = carpeta_archivo[0]
                archivo = carpeta_archivo[1]
                s3res.Bucket(os.environ["BUCKET"]).download_file(video, archivo)
                destino = carpeta + "/converted/" + (archivo.rsplit(".", 1)[0]) + '.mp4'
                archivo_destino = "c_" + (archivo.rsplit(".", 1)[0]) + ".mp4"
                #destino = "/home/SIS/fd.guevara1054/Videos/" + str(segundos) + "_" + record.rsplit(".", 1)[0] + "_" + fechaInicial.strftime("%Y%m%d-%H:%M:%S") + "_" + fechaConversion.strftime("%Y%m%d-%H:%M:%S") + ".mp4"
                ff = ffmpy.FFmpeg(
                    inputs={archivo: None},
                    outputs={archivo_destino: "-y -c:v libx264 -c:a aac -strict -2 -preset ultrafast"}
                )
                print("COMANDO: " + ff.cmd)
                ff.run() 
                s3cli.upload_file(archivo_destino, os.environ["BUCKET"], destino, ExtraArgs={'ACL':'public-read'})
                videos.update_one(
                    { "id": id },
                    { "$set": { "status": "T", "converted_video": "videos/converted/" + (archivo.rsplit(".", 1)[0] + '.mp4') } }
                )
                os.remove(archivo)
                os.remove(archivo_destino)
                mensaje.delete()
                sg = sendgrid.SendGridAPIClient(apikey=os.environ.get('SENDGRID_API_KEY'))
                from_email = Email("smarttools08@gmail.com")
                subject = "¡Tu video está listo para ver!"
                to_email = Email(videoObj["email"])
                concursoObj = concursos.find_one({"id":videoObj["contest_id"]})
                concurso = concursoObj["name"]
                url = concursoObj["url"]
                usuario = videoObj["first_name"]
                content = Content("text/html", "¡Hola, " + usuario + "! <br><br> El video que postulaste para el concurso " + concurso + " está listo para ser visualizado en el portal. El enlace al concurso es " + os.environ["APP_URL"] + url + ".<br><br> ¡Mucha suerte!")
                mail = Mail(from_email, subject, to_email, content)
                response = sg.client.mail.send.post(request_body=mail.get())
                print(response.status_code)
                print("Se ha transformado el video de la ruta " + video + ".")
    except (ffmpy.FFExecutableNotFoundError) as error:
        print("FFmpeg no encontrado.")
    except (ffmpy.FFRuntimeError) as error:
        print("Error transformando el archivo: " + str(error))   
    except (Exception) as error:
        print("Error: " + str(error))
    finally:
        if client is not None:
            client.close()
            print('Database connection closed.')
        os.environ["TRANSFORMANDO"] = "NO"
