def get_release(self):
    import datetime
    return datetime.datetime.now().strftime('%Y-%m-%d-%H:%M')
