from celery import Celery
from celery.schedules import crontab
from pymongo import MongoClient
from bson import ObjectId
import boto3
import os
import ffmpy
import smtplib
import datetime
import sys
import email.utils
from email.mime.text import MIMEText as text
from email.mime.multipart import MIMEMultipart as mp

app = Celery()

@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # Transformar archivos cada X segundos
    sender.add_periodic_task(1.0, test2.s("hello"), name='add every 5')
    
@app.task
def test2(arg):
    if os.environ["TRANSFORMANDO"] == "SI":
        return
    try:
        client = None
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
            client = MongoClient(os.environ["DB_HOSTNAME"], int(os.environ["DB_PORT"]))
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
                video = path_videos + video
                print("RUTA POST " + video)
                carpeta_archivo = os.path.split(os.path.abspath(video))
                carpeta = carpeta_archivo[0]
                archivo = carpeta_archivo[1]
                destino = carpeta + "/converted/" + (archivo.rsplit(".", 1)[0] + '.mp4')
                #destino = "/home/SIS/fd.guevara1054/Videos/" + str(segundos) + "_" + record.rsplit(".", 1)[0] + "_" + fechaInicial.strftime("%Y%m%d-%H:%M:%S") + "_" + fechaConversion.strftime("%Y%m%d-%H:%M:%S") + ".mp4"
                ff = ffmpy.FFmpeg(
                    inputs={video: None},
                    outputs={destino: "-y -c:v libx264 -c:a aac -strict -2 -preset ultrafast"}
                )
                print("COMANDO: " + ff.cmd)
                ff.run() 
                videos.update_one(
                    { "id": id },
                    { "$set": { "status": "T", "converted_video": "videos/converted/" + (archivo.rsplit(".", 1)[0] + '.mp4') } }
                )
                mensaje.delete()
                SENDER = os.environ["MAIL_USER"]
                SENDERNAME = 'SmartTools'
                RECIPIENT  = videoObj["email"]
                USERNAME_SMTP = os.environ["SMTP_USER"]
                PASSWORD_SMTP = os.environ["SMTP_PASS"]
                HOST = "email-smtp.us-east-1.amazonaws.com"
                PORT = 587
                SUBJECT = '¡Tu video está listo para ver!'
                gmail_user = 'smarttools08@gmail.com'  
                concursoObj = concursos.find_one({"id":videoObj["contest_id"]})
                concurso = concursoObj["name"]
                url = concursoObj["url"]
                usuario = videoObj["first_name"]
                BODY_TEXT = ("¡Hola, " + usuario + "! \n\n El video que postulaste para el concurso " + concurso + " está listo para ser visualizado en el portal. El enlace al concurso es " + os.environ["APP_URL"] + url + ".\n\n ¡Mucha suerte!"
                        )
                msg = mp('alternative')
                msg['Subject'] = SUBJECT
                msg['From'] = email.utils.formataddr((SENDERNAME, SENDER))
                msg['To'] = RECIPIENT
                part1 = text(BODY_TEXT, 'plain')
                msg.attach(part1)
                server = smtplib.SMTP(HOST, PORT)
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(USERNAME_SMTP, PASSWORD_SMTP)
                server.sendmail(SENDER, RECIPIENT, msg.as_string())
                server.close()
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
