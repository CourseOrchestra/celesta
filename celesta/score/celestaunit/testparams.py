# coding=UTF-8
from java.util import Properties
import os

CELESTA_PROPERTIES = Properties()
CELESTA_PROPERTIES.put('score.path', os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
CELESTA_PROPERTIES.put('pylib.path', '/')  # Данная настройка необходима только в контексте запуска java приложения
CELESTA_PROPERTIES.put("h2.in-memory", "true")
#CELESTA_PROPERTIES.put('database.connection', 'jdbc:postgresql://127.0.0.1:5432/celesta?user=postgres&password=123')

# Список модулей для импорта модулем celestaunit.py
INITIALIZING_GRAINS = []
