
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from pprint import pprint
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd


#Abrir o site
url = "https://www.youtube.com.br/"
driver = webdriver.Chrome()
driver.get(url)

#Seleciona e clica no buscar
#Precisa ajustar so funciona com o inspecionar elemento(12) ativado no campo inicialmente
try:
    buscar = driver.find_element(By.XPATH, "/html/body/ytd-app/div[1]/div/ytd-masthead/div[4]/div[2]/ytd-searchbox/form/div[1]/div[1]/div/div[2]/input")
    buscar.click()
except:
    buscar = driver.find_element(By.XPATH, '/html/body/ytd-app/div[1]/div/ytd-masthead/div[4]/div[2]/ytd-searchbox/form/div[1]/div[1]/div/div[2]/input')
    buscar.click()

#Limpa campo buscar
buscar.clear()

#Efetua a pesquisa
#buscar.send_keys('Selic')
buscar.send_keys('finanças pessoais')

#Clicar em pesquisar item da busca
driver.find_element(By.XPATH, "/html/body/ytd-app/div[1]/div/ytd-masthead/div[4]/div[2]/ytd-searchbox/button/yt-icon").click()


#Codigo de extração e ingestão em banco de dados
#Melhorar para pegar classe do campo e não o xpath para não ficar alterando o caminho

#Cria um DataFrame vazio
colunas = ['nome', 'periodo', 'comentario', 'titulo', 'canal', 'url']
df_extracao = pd.DataFrame(columns=colunas)

#Inicio ou reinicio segundo a order do video
index_video = 1 # pode ser reiniciado de onde parou

#Conecta no db Mysql
from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://root:senha@localhost/database")

#inicia Scraping até dar erro
while True:
    try:
        #Clica no video conforme o index
        driver.find_element(By.XPATH, '/html/body/ytd-app/div[1]/ytd-page-manager/ytd-search/div[1]/ytd-two-column-search-results-renderer/div/ytd-section-list-renderer/div[2]/ytd-item-section-renderer[1]/div[3]/ytd-video-renderer['+str(index_video)+']/div[1]/div/div[1]/div/h3/a/yt-formatted-string').click()

        #lista para receber todos os comentarios do canal
        comentarios = []

        #Inicia index do comentarios
        i = 1

        #rola a pagina 3 vezes para acessar os comentarios
        driver.execute_script("window.scrollBy(0,5000)")
        time.sleep(5)
        driver.execute_script("window.scrollBy(0,5000)")
        time.sleep(5)
        driver.execute_script("window.scrollBy(0,5000)")
        time.sleep(5)

        #Identifica o total de comentarios do video e estabelece o total de loop para extrair os comentarios
        tt_comentario = driver.find_element(By.XPATH,'/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-comments/ytd-item-section-renderer/div[1]/ytd-comments-header-renderer/div[1]/h2/yt-formatted-string/span[1]').text
        tt_comentario = int(tt_comentario.replace('.',''))
        loop_pagina = round(int(tt_comentario)/20)

        #Loop para extração dos comentarios
        for lp in range(loop_pagina):

            #Rola a pagina para acessar mais comentarios
            driver.execute_script("window.scrollBy(0,5000)")
            time.sleep(5)

            #Extrair até 20 comentarios antes de rolar a pagina novamente
            for l in range(20): #media de comentario disponivel antes de dar erro
                lista = []
                try:
                    #Nome
                    lista.append(driver.find_element(By.XPATH, '/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-comments/ytd-item-section-renderer/div[3]/ytd-comment-thread-renderer['+str(i)+']/ytd-comment-renderer/div[3]/div[2]/div[1]/div[2]/h3/a/span').text)
                    #Periodo
                    lista.append(driver.find_element(By.XPATH, '/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-comments/ytd-item-section-renderer/div[3]/ytd-comment-thread-renderer['+str(i)+']/ytd-comment-renderer/div[3]/div[2]/div[1]/div[2]/yt-formatted-string/a').text)
                    #Comentario
                    lista.append(driver.find_element(By.XPATH, '/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-comments/ytd-item-section-renderer/div[3]/ytd-comment-thread-renderer['+str(i)+']/ytd-comment-renderer/div[3]/div[2]/div[2]/ytd-expander/div/yt-formatted-string').text)
                    #Titulo
                    titulo = driver.find_element(By.XPATH, '/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-watch-metadata/div/div[1]/h1/yt-formatted-string').text
                    lista.append(titulo)
                    #Canal
                    canal = driver.find_element(By.XPATH, '/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-watch-metadata/div/div[2]/div[1]/ytd-video-owner-renderer/div[1]/ytd-channel-name/div/div/yt-formatted-string/a').text
                    lista.append(canal)
                    #Url
                    lista.append(driver.current_url)
                    comentarios.append(lista)
                    i = i + 1
                except:
                    pass
            #Acompanhamento de extração do canal
            print('Video: ',titulo,'| comentarios extraido: ',len(comentarios), 'loop: '+str(lp)+'/'+str(loop_pagina))

            #Se o Youtube parar com a rolagem e a extração não retornar mais comentarios parar esse canal
            if len(lista) == 0:
                break

        #ingestão de comentarios do canal no Mysql
        df_extracao = pd.DataFrame(comentarios, columns=colunas)
        df_extracao.to_sql(name='scraping_ytube_financas_pessoais',con=engine, schema='tcc', if_exists='replace')

        #Retorna para pagina anterior
        driver.back()

        #Adiciona 1 no index_video para acessar o proximo na lista
        index_video = index_video+1

        #Monitoramento de total de comentarios extraido do video
        print('Video:',titulo,' - Canal:',canal,' - tt_comentario:',df_extracao.shape[0])

    #Caso erro print o erro e adciona mais um para mudar de video
    except FileNotFoundError as error:
        print(error)
        index_video = index_video+1

