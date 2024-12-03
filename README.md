# Lentochka (Ленточка) - Python Утилита Резервного Копирования

## Описание
`Lentochka` — это комплексный Python-пакет для автоматизации резервного копирования на ленточные накопители в операционных системах LINUX и DSMC.

## Возможности
- Автоматизированное резервное копирование на ленточные накопители
- Поддержка DSMC (Distributed System Management Console)
- Гибкая настройка через конфигурационный файл
- Мониторинг и логирование процессов резервного копирования

## Установка

1. Скопируйте архив `lentochka_offline.tar.gz` на целевую машину
2. Распакуйте архив:
   ```bash
   tar -xzvf lentochka_offline.tar.gz
   ```

3. Установите Lentochka:
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
