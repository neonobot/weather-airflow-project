import datetime as dt
import requests



# Получение данных о средней температуре в Москве
def get_weather_data():
    url = "https://api.openweathermap.org/data/2.5/weather"
    api_key = os.getenv('OPENWEATHER_API_KEY', '3cffcb0d287e0d05d6d6f7217bcebd5a')
    params = {
        'lat': 55.75,
        'lon': 37.62,
        'appid': api_key
    }

    response = requests.get(url, params=params)

    data = response.json()

    temp_max = (data['main']['temp_max']) - 273.15
    temp_min = (data['main']['temp_min']) - 273.15
    avg_temp = round(((temp_min + temp_max) / 2), 2)
    today = dt.date.today()

    new_data = {'avg_temp': avg_temp, 'date': today, 'change_temp': None, 'prediction': None}

    print(
        f'Максимальная температура - {temp_max}, Минимальная температура - {temp_min}, Средняя температура - {avg_temp}, Дата - {today}',
        data)

    return new_data



#Тестовая функция для локальной проверки
def test_get_weather_data():
    try:
        data = get_weather_data()
        print(f"Тестовые данные: {data}")
        return data
    except Exception as e:
        print(f"Ошибка при получении данных: {e}")
        return None


if __name__ == "__main__":
    # Для локального тестирования
    test_get_weather_data()

