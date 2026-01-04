import os
import json
import dill
import pandas as pd

MODEL_PATH = 'data/models/cars_pipeline.pkl'
TEST_DIR = 'data/test'
PREDICTIONS_PATH = 'data/predictions/predictions.csv'

def predict():
    # Загружаем модель
    with open(MODEL_PATH, 'rb') as f:
        model = dill.load(f)

    # Читаем все JSON-файлы
    records = []
    for fname in os.listdir(TEST_DIR):
        if fname.endswith('.json'):
            with open(os.path.join(TEST_DIR, fname), 'r') as f:
                data = json.load(f)
                records.append(data)

    # Преобразуем в DataFrame
    df = pd.DataFrame(records)

    # Предсказываем
    preds = model.predict(df)

    # Сохраняем
    df_out = pd.DataFrame({'prediction': preds})
    os.makedirs(os.path.dirname(PREDICTIONS_PATH), exist_ok=True)
    df_out.to_csv(PREDICTIONS_PATH, index=False)

    print(f"Saved predictions to {PREDICTIONS_PATH}")


if __name__ == '__main__':
    predict()
