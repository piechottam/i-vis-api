.. _install_docker:

Install from docker
===================

Make sure `docker`_ is installed on system.

1. Contact `<mailto:michael.piechotta@informatik.hu-berlin.de>`_ to get I-VIS in a docker container (We are working on
   adding a Docker Hub release).
   Unpack the supplementary files that contain the i-vis.conf, i-vish.sh and the start.sh.

2. Load image:
    .. code-block:: console

       $ docker load < predict-i-vis-api-container.tar.gz

3. Adjust config in i-vis.conf: add plugin accounts, enable/disable plugins.
   Copy the conf file where start.sh is expecting it (into the /conf folder)

5. Adjust start.sh: set where volumes should be mounted (base directory)
   and set desired port number.

6. Start the container: `$ ./start.sh`
   This creates a mysql database.
   This will create a mysql database.
   You should see that the mysql server could be started.

7. Create I-VIS tables with: `$ ./i-vis.sh cli init db`

8. Update plugin repository with: `$ ./i-vis.sh cli plugin update`

9. Install upgrades with: `$ ./i-vis.sh cli plugin upgrade`

10. Start I-VIS API with: `$ ./i-vis.sh api run`

11. Create account and API-TOKEN:
    .. code-block:: console

       $ ./i-vis.sh cli user create USERNAME MAIL
       $ ./i-vis.sh cli list # note your API-Token in column "Token"

    Attach it to all requests to I-VIS http:<HOST>:PORT/api?token=<API-TOKEN>

11. Check http(s):<HOST>:PORT/api/swagger for available endpoints.

Optional:
* SSL