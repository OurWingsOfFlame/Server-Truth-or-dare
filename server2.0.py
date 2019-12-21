"""
Подключаемся к базе данных --> запускаем собственный сервер --> создаём серверный сокет -->
--> ожидаем подключение клиента --> подключаем его и записываем клиентский сокет --> создаём объект класса Gamer -->
--> запускаем поток --> запускаем цикл взаимодействия с клиентом, пока клиент не отключится -->
--> читаем запрос пользователя, приводим к нормальному виду -->
--> усыпляем его, чтобы конкурентно обработать запросы других пользователей --> выполняем его --> отправляем ответ -->
--> ждём новый запрос --> ... --> закрываем сокет, поток закроется сам


Главный поток (соответсвтующий главному процессу сервера) занимается лишь приемом входящих подключений.
Создав очередной клиентский сокет (accept_client_conn), он запускает дополнительный поток, входной точкой, для которого
является простая версия serve_client(). Выполнив обработки запроса, дополнительный поток завершается
"""


import socket  # Инфраструктура сокетов
from datetime import datetime  # Для получения текущего времени
from time import sleep  # Для "усыпления" клиента
import threading  # Всё для потоков
import pymysql  # Взаимодействие с базой
from random import choice  # Выбор случайного элемента из списка

connection = pymysql.connect(host='192.168.1.25',  # Устанавливаем соединение с базой данных
                             user='dasha',
                             password='1111',
                             db='1111',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
print("successful connection to bd")
cursor = connection.cursor()  # This is the object you use to interact with the database.


# Класс, объектами которого будут подключенные игроки. После отключения игрока объект класса зануляется
class Gamer:  # Поля у полностью созданного игрока: сокет, логин, пройденные вопросы, локация, возрастной режим
    def __init__(self, sock):  # Для создания объекта класса необходимо сразу же записывать его сокет
        self.socket = sock

    # Дефолтные значения:
    login = 'dasha'
    ready_task = []
    loc = 'all'
    age = 0

    def ini(self, login, ready_task):  # Выполняем эту функцию, когда установлено соответсвие между сокетом и игроком
        # из нашей базы данных
        self.login = login
        self.ready_task = ready_task  # Сюда же запишем вопросы из dislike, чтобы не обращаться каждый раз к этому
        # столбцу при получении нового задания, а с пройденными заданиями сравнивать в любом случае необходимл

    def start_gamer(self, loc, age):  # Функция, выполняемая при нажатии игрока на "старт", записываем выбранный
        # возрастной режим и локацию
        self.loc = loc
        self.age = age


def run_server(port=53211):
    serve_sock = create_serve_sock(port)  # Создаём соккет
    cid = 0  # Колличество потоков
    while True:
        client_sock = accept_client_conn(serve_sock, cid)  # Подтверждаем подключения пользователя
        user = Gamer(client_sock)  # Создаём новый объект класса
        print(f'{datetime.now()}: Подключили клиента')
        t = threading.Thread(target=serve_client,
                             args=(user, cid))  # Создание потока (по умолчанию будет выполнена функция serve_client
        # с аргументами (user, cid))
        print(f'{datetime.now()}: Создали поток')
        t.start()
        print(f'{datetime.now()}: Запустили этот поток')
        cid += 1
        print(f'{datetime.now()}: Увеличили число потоков')  # На каждого пользователя свой поток


def create_serve_sock(serve_port):  # Создание сокета в сети
    serve_sock = socket.socket(socket.AF_INET,  # address family ipv4
                               socket.SOCK_STREAM,  # connection oriented TCP protocol
                               proto=0)
    serve_sock.bind(('', serve_port))  # Инициализирует ip-адрес и порт, при этом проверяется, не занят ли порт
    # другой программой
    serve_sock.listen()
    return serve_sock


def accept_client_conn(serve_sock, cid):  # Подтверждение подключения
    client_sock, client_addr = serve_sock.accept()
    print(f'{datetime.now()}'f': Client #{cid} connected 'f'{client_addr[0]}:{client_addr[1]}')
    return client_sock


def serve_client(user, cid):  # Взаимодействие сервера с клиентом
    flag = True
    while flag:  # Цикл работает, пока клиент отправляет запросы

        request = read_request(user, cid)  # Получаем от пользователя запрос и выполняем его
        print(f'{datetime.now()}'f': Прочитали сообщение пользователя #{cid}')
        if request is None:  # Если запрос пустой
            print(f'{datetime.now()}'f': Client #{cid} disconnected')
            user.socket.close()
            flag = False
        else:
            print(f'{datetime.now()}'f': Обработали запрос пользователя #{cid}')
            handle_request()  # Блокируем пользователя, чтобы сервер мог перейти к выполнению запросов других игроков
            print(f'{datetime.now()}'f': Подготовили ответ для пользователя #{cid}')
            write_response(user.socket, request, cid)  # Отправляем клиенту ответ


def read_request(user, cid):  # Получение пользовательского запроса и его выполнение
    try:
        while True:
            chunk = user.socket.recv(2048)  # Получение данных из сокета. 512 - размер буффера
            chunk = parser(chunk)  # Приводим байтовую строку к виду списка, с которым удобно работать
            print(f'{datetime.now()}'f': Запрос пользователя #{cid}: {chunk}')
            if not chunk:  # Если запрос пустой
                # Клиент преждевременно отключился.
                return None
            if chunk[0] == 'next_dare':  # Следующее действие
                task_id, task = next_task(1, user)
                user.ready_task.append(task_id)  # Заносим действие в список пройденных заданий
                return task
            elif chunk[0] == 'next_quest':  # Следующий вопрос
                task_id, task = next_task(0, user)
                user.ready_task.append(task_id)  # Заносим вопрос в список пройденных заданий
                return task
            elif chunk[0] == 'sign_up':  # Регистрация нового пользователя
                flag, user = sign_up(chunk[1], chunk[2], user)  # В функцию передаём логин, пароль и объект игрока
                return flag
            elif chunk[0] == 'log_in':  # Вход в игру зарегестрированного игрока
                flag, user = log_in(chunk[1], chunk[2], user)  # В функцию передаём логин, пароль и объект игрока
                return flag
            elif chunk[0] == 'dislike':  # Отметить вопрос как непонравившийся
                dislike(user)
                return True
            elif chunk[0] == 'start':  # Выбирает место и возраст
                user.start_gamer(chunk[1], chunk[2])  # Место, возраст
                return True
            elif chunk[0] == 'new_task':  # Добавление нового вопроса в базу для модерации
                new_task(chunk[1], chunk[2])  # Правда/действие, само задание
                return True
            else:
                print(f'{datetime.now()}'': Неожиданный запрос с пользовательской стороны')
                print(chunk)
                return "Error"

    except ConnectionResetError:
        # Соединение было неожиданно разорвано.
        return None
    except:
        raise  # Если отловили какое-то исключение, отправляем его в вышестоящую функцию


def handle_request():  # Заставляем пользователя зависнуть на 5мс, чтобы перейти к выполнению запросов других
    # пользователей
    sleep(0.01)


def write_response(client_sock, response, cid):  # Отправка ответа клиентской стороне
    print(f'{datetime.now()}'f': Отправляем сообщение пользователю #{cid}')
    response = str(response)
    client_sock.sendall(bytes(response, 'utf-8'))
    print(f'{datetime.now()}'f': Отправили ответ пользователю #{cid}')


def sign_up(login, password, user):  # Регистрация нового пользователя
    sql = "SELECT * FROM clients WHERE name = %s"  # Создаём SQL запрос, получающий строку с логином, которых хочет
    # зарегестрировать пользователь, если такая уже существует
    cursor.execute(sql, login)  # Выполняем запрос в бд
    result = cursor.fetchone()  # Получаем строку, которые отправила бд (список словарей)
    if result:  # Если она не пустая, значит такой логин уже занят
        return [False, user]  # Возвращаем ошибку
    insert_query = "INSERT INTO clients (name, password)  VALUES (%s, %s) "  # Добавляем в таблицу нового пользователя
    cursor.execute(insert_query, (login, password))
    connection.commit()  # Делаем коммит базы
    user.ini(login, [])  # Записываем в объект логин и пустой список непонравившихся заданий, т.к. новый игрок ещё ни
    # одно задание не видел -> этот список у него в бд всё равно пустой, можно не тратить ресурсы и заполнить автоматом
    return [True, user]


def log_in(login, password, user):  # Вход зарегестрированного игрока
    sql = "SELECT * FROM clients WHERE name = %s"  # Создаём SQL запрос, получающий всю таблицу пользователей
    cursor.execute(sql, login)  # Выполняем запрос
    row = cursor.fetchone()  # Получаем строку таблицы с таким логином
    flag = False
    if not row:  # Если строка оказалась пустой, значит пользователя с таким логином не существует
        return flag, user
    if row['password'] == password:  # Если введённые логин и пароль совпадают с
        # хранящимися
        flag = True
        if row['dislike_task']:  # Если у пользователя уже есть вопросы, которые ему раньше не понравились
            s = row['dislike_task']
            s = s.split(' ')
            s = list(map(int, s))
            user.ini(login, s)  # Заполняем поля объкта, сохраняя непонравившиеся
            # вопросы к уже пройденным
        else:
            user.ini(login, [])  # Заполняем поля объекта, считая, что он не видел ни одного вопроса
    return flag, user  # Флаг false, если не нашли пару логин-пароль, совпадающих с веденными


def new_task(type_flag, string):  # Добавление нового задания, type_flag отвечает, вопрос это или действие
    insert_query = "INSERT INTO added_task (flag, task) VALUES (%s, %s) "
    cursor.execute(insert_query, (int(type_flag), string))
    connection.commit()
    return True


def next_task(type_flag, gamer):  # Получение нового задания, type_flag отвечает, вопрос это или действие
    if type_flag == '0':
        location = 'all'
    else:
        location = gamer.loc
    sql = "SELECT * FROM tasks WHERE flag = %s and (location = %s  or location = 'all') and age = %s"
    cursor.execute(sql, (int(type_flag), location, gamer.age))
    results = cursor.fetchall()
    flag = True
    while flag:  # Будем выбирать строку, пока она нас не устроит
        task = choice(results)  # Случайным образом выбираем строку
        if int(task['id']) not in list(gamer.ready_task):  # Если пользователь ещё не встречал этот вопрос
            flag = False
    return [task['id'], task['task']]


def dislike(user):  # Добавление вопроса в список непонравившихся
    sql_query = "SELECT * FROM clients WHERE name = %s"  # Получаем из таблицы строку этого пользователя
    cursor.execute(sql_query, user.login)
    row = cursor.fetchone()
    num = user.ready_task[-1]  # Берем номер последнего задания
    row = set_key(row, 'dislike_task', int(num))  # Добавляем в имеющееся множество непонравившихся вопросов ещё один
    sql = "UPDATE clients set dislike_task = %s WHERE name = %s"
    cursor.execute(sql, (row['dislike_task'], user.login))
    connection.commit()
    return True


def set_key(dictionary, key, value):  # Внутренняя функция, позволяет добавить в имеющийся словарь по ключю обнавить
    # значение элементов
    if key not in dictionary:
        dictionary[key] = value
    elif type(dictionary[key]) == list:
        dictionary[key].append(value)
    else:
        dictionary[key] = (dictionary[key]) + ' ' + str(value)
    return dictionary


def parser(s):  # Служебная функция, приводим байтовую строку к виду списка
    if s:
        s = s.decode('utf-8')
        s = s[1:-1]
        s = list(map(str, s.split(',')))
    return s


run_server()  # Запускаем сервер
print(f'{datetime.now()}'': Сервер запущен')


