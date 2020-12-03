Data_scientia
==============================

The project
==============================
In this project you can find a pipeline to train and 
deploy a model for predict the hospital occupancy in 
the Ciudad de México.


Getting the data
==============================
In the folder data_scientia/data, execute the scripts 
capacidad_hospitalaria.py and covid_municipalities.py. 
These to scripts downloaded the daily status of hospital 
occupancy and the daily cases of covid cases in Ciudad 
de México.

#Creating features
==============================
In the folder data_scientis/features execute the script
train_data.py. These create the training dataset calculating
differents time series features using the municipality covid
cases near from the differents hospitals. 

#Train the model
==============================
In the notebook experiment.ipynb, you can see the pipeline
to train, validate, visualize and see the feature
importance of the train model.

#Deploy the model
==============================
To deploy a new model, you have to be positioned in the 
chalice folder and execute: `chalice deploy --connection-timeout 500 `

#Notebooks
==============================
Project Organization
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── data_scientia      <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │    │
    │   │    ├── covid_municipalities.py
    │   │    ├── municipios.py
    │   │    └── capacidad_hospitalary.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │    │ 
             ├── covid_municipalities.py
             ├── critical_peaks.py
             ├── features_timeseries.py
             ├── hospital.py
             ├── target_days_to_peak.py
             └── train_data.py
    │   │    
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.readthedocs.io


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>

Data Mexico
