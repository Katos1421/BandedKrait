# Lentochka (Ленточка) - Python Утилита Резервного Копирования

![изображение](https://github.com/user-attachments/assets/4a78ddbd-f6bd-46a1-95f8-74ba9594c7a3)



## Описание
`Lentochka` — это комплексный Python-пакет для автоматизации резервного копирования на ленточные накопители в операционных системах LINUX и DSMC.

## Возможности
- Автоматизированное резервное копирование на ленточные накопители
- Поддержка DSMC (Distributed System Management Console)
- Гибкая настройка через конфигурационный файл
- Мониторинг и логирование процессов резервного копирования

## Установка

1. Распакуйте архив `lentochka_offline.tar.gz`
2. Установите Lentochka:
   ```bash
   pip install lentochka-0.1-py3-none-any.whl
   ```

## Использование
```bash
lentochka  # Основной CLI
```

## Системные требования
- Python 3.6+
- Linux
- Установленный DSMC

## Зависимости
- configparser
- psutil

## Удаление
```bash
pip uninstall lentochka
```

## Версия
v0.1 - Расширенный функционал резервного копирования
