from dynaconf import Dynaconf

settings = Dynaconf(environments=True, envvar_prefix=False, load_dotenv=True)
