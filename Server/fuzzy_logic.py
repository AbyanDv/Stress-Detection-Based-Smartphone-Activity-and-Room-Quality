import numpy as np
import skfuzzy
from skfuzzy import control as ctrl

def fuzzy_level(hours):
    """
    Tentukan level penggunaan dan pesan feedback berdasarkan jam pemakaian.
    """
    if hours < 1:
        level = "rendah"
        message = "Waktu layar kamu masih sangat sehat 👍"
    elif 1 <= hours < 3:
        level = "sedang"
        message = "Waktu layar kamu masih dalam batas wajar 😊"
    elif 3 <= hours < 5:
        level = "tinggi"
        message = "Mulai kurangi penggunaan HP ya, mata perlu istirahat 👀"
    else:
        level = "sangat tinggi"
        message = "Kamu terlalu lama di depan layar hari ini 😣"
    
    # kembalikan dua nilai: level dan pesan
    return level, message
