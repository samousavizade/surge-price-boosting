# FROM python:3.11-slim-buster
FROM localhost:5000/python:3.11-slim-buster


# RUN su -
RUN sed -i 's|http://deb.debian.org/debian|http://archive.debian.org/debian|g' /etc/apt/sources.list && \
sed -i '/security.debian.org/d' /etc/apt/sources.list && \
echo 'Acquire::Check-Valid-Until "false";' > /etc/apt/apt.conf.d/99no-check-valid-until

WORKDIR /app

COPY requirements.txt ./
RUN pip install -r requirements.txt
RUN apt-get update
RUN apt-get install -y curl
RUN curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc
RUN curl https://packages.microsoft.com/config/debian/10/prod.list | tee /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql18
RUN ACCEPT_EULA=Y apt-get install -y mssql-tools18
RUN echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
RUN rm /bin/sh && ln -s /bin/bash /bin/sh
RUN source ~/.bashrc
RUN apt-get install -y unixodbc unixodbc-dev libgssapi-krb5-2
COPY ../surge_price_boosting .
EXPOSE 8070

CMD ["python", "main.py"]
