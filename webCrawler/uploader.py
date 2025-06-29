import subprocess
import os

container_name = "namenode"
local_folder = "wiki_data"
container_folder = "/wiki_data"
hdfs_target_dir = "/user/root/wiki_data"

def run_command(cmd):
    try:
        print(f"Running: {' '.join(cmd)}") #Imprime el comando que se desea correr
        subprocess.run(cmd, check=True) # Ejecuta el comando y verifica si hubo errores
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")

def upload_to_hdfs():
    if not os.path.exists(local_folder):
        print(f"Folder '{local_folder}' not found.")
        return

    #flujo de trabajo:
    #1. Inserta el documento en el contenedor
    #2. Crea el directorio en HDFS
    #3. Sube el documento al HDFS
    #4. Verifica que el documento se haya subido correctamente

    print(f"Uploading file '{local_folder}'...")
    run_command(["docker", "cp", local_folder, f"{container_name}:{container_folder}"])

    print(f" Creating HDFS directory '{hdfs_target_dir}'...")
    run_command(["docker", "exec", container_name, "hdfs", "dfs", "-mkdir", "-p", hdfs_target_dir])

    print(f"Uploading files from container to HDFS...")
    run_command(["docker", "exec", container_name, "hdfs", "dfs", "-put", "-f", "/wiki_data/wiki_data.jsonl", hdfs_target_dir])

    print(f"Verifying files in HDFS:")
    run_command([
        "docker", "exec", container_name, "hdfs", "dfs", "-ls", hdfs_target_dir
    ])

upload_to_hdfs()