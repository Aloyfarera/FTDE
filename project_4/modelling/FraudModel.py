import os
import pickle
import pandas as pd

def prepOneHotEncoder(df, col, pathPackages):
    oneHotEncoder = pickle.load(open(pathPackages + '\\' + 'prep' + col + '.pkl', 'rb'))
    dfOneHotEncoder = pd.DataFrame(oneHotEncoder.transform(df[[col]]).toarray(),
                                   columns=[col + "_" + str(i+1) for i in range(len(oneHotEncoder.categories_[0]))])
    df = pd.concat([df.drop(col, axis=1), dfOneHotEncoder], axis=1)
    return df

def prepStandardScaler(df, col, pathPackages):
    scaler = pickle.load(open(pathPackages + '\\' + 'prep' + col + '.pkl', 'rb'))
    df[col] = scaler.transform(df[[col]])
    return df

def runModel(data, path):
    pathPackages = os.path.join(path, "packages") + ""
    col = pickle.load(open(os.path.join(pathPackages, 'columnModelling.pkl'), 'rb'))
    df = pd.DataFrame(data, index=[0])
    df = df[col]

    df = prepOneHotEncoder(df, 'type', pathPackages)

    cols_to_scale = ['amount', 'oldbalanceOrg', 'newbalanceOrig', 'oldbalanceDest', 'newbalanceDest']
    for col in cols_to_scale:
        df = prepStandardScaler(df, col, pathPackages)

    X = df.values
    model = pickle.load(open(os.path.join(pathPackages, 'modelFraud.pkl'), 'rb'))
    y = model.predict(X)[0]
    if y == 0:
        return "White List"
    else:
        return "Fraud"

if __name__ == "__main__":
    # Contoh data baru untuk diprediksi
    new_data = {
        'step': 1,
        'type': 'PAYMENT',
        'amount': 9839.64,
        'oldbalanceOrg': 170136.0,
        'newbalanceOrig': 160296.36,
        'oldbalanceDest': 0.0,
        'newbalanceDest': 0.0
    }

    path = os.getcwd()
    prediction = runModel(new_data, path)
    print(f"Prediction: {prediction}")