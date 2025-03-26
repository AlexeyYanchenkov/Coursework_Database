from configparser import ConfigParser

def config(file_name, section='postgres'):
    parser = ConfigParser()
    parser.read(file_name)

    try:
        params = parser.items(section)
        db = {}
        for param in params:
            db[param[0]] = param[1]
        return db
    except:
        print(f"Error")