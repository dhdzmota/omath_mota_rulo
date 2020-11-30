#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt
import datetime
import geopandas
import pandas as pd
import PIL
import os
import io


from data_scientia.data.covid_municipalities import get_state
from data_scientia.data import capacidad_hospitalaria
from data_scientia import config


INIT_TIME = '2020-04-01'
LONGITUDE_MAX = -99
LONGITUDE_MIN = -99.5
LATITUDE_MAX = 19.5
LATITUDE_MIN = 19

MAP_PATH = os.path.join(
    config.MAPS_DIR,
    'alcaldias/alcaldias.shp')

GIF_PATH = os.path.join(
    config.FIGURES_DIR,
    'contagios.gif'
)


def plot_capacidad(data, ax, status):
    """
    """
    colors = {'Buena': 'green', 'Media': 'orange', 'Crítica': 'red'}
    c = data[data['estatus_capacidad_hospitalaria'] == status]

    gdf = geopandas.GeoDataFrame(
        c,
        geometry=geopandas.points_from_xy(c.longitude, c.latitude))

    gdf.plot(ax=ax, color=colors[status], markersize=10)


def contagios_gif():
    """
    """
    print(config.MAPS_DIR)
    print("Getting Ciudad de México data...")
    delegaciones = get_state('Ciudad de México')

    delegaciones = delegaciones[delegaciones['Time'] > INIT_TIME]
    capacidad = capacidad_hospitalaria.get()
    capacidad = capacidad[(capacidad['longitude'] > LONGITUDE_MIN)
                          & (capacidad['longitude'] < LONGITUDE_MAX)
                          & (capacidad['latitude'] > LATITUDE_MIN)
                          & (capacidad['latitude'] < LATITUDE_MAX)]

    print('Getting shape file...')
    world = geopandas.read_file(MAP_PATH, encoding='utf-8')

    image_frame = []

    print("Generating daily incidents...")
    for day, subset in capacidad.set_index('fecha').groupby(pd.Grouper(freq='1D')):
        print(day)
        max_day = day
        min_day = day - datetime.timedelta(7)
        week = delegaciones[(delegaciones['Time'] > min_day) & (delegaciones['Time'] < max_day)]

        daily_cases = pd.DataFrame(week.groupby('Municipality')['Daily Cases'].sum())

        capacidad_day = capacidad[capacidad['fecha'] == day]

        w = world.merge(daily_cases, left_on='nomgeo', right_index=True)
        ax = w.plot(figsize=(15, 10), column='Daily Cases', cmap='OrRd', edgecolor='black', vmax=1471,
                    legend=True)

        plot_capacidad(capacidad_day, ax, 'Buena')
        plot_capacidad(capacidad_day, ax, 'Media' )
        plot_capacidad(capacidad_day, ax, 'Crítica')

        ax.set_title('Casos en 7 días\n' + str(day.date()), fontsize=25)

        plt.axis('off')
        plt.show()

        img = ax.get_figure()
        f = io.BytesIO()
        img.savefig(f, format='png', bbox_inches='tight', quality=95)
        f.seek(0)
        image_frame.append(PIL.Image.open(f))

        image_frame[0].save(GIF_PATH, format='GIF', append_images=image_frame[1:],
                            save_all=True, duration=400, loop=1, quality=95)

        f.close()


if __name__ == '__main__':

    contagios_gif()
