# Third-party notices

nostrology-data is licensed under the GNU Affero General Public License, version 3 or (at your option) any later version: see [LICENSE](LICENSE). It also ships one file built from other projects, whose licenses still apply to it.

## reverse_geocoder 1.5.1 and the GeoNames gazetteer

`deploy/dagster/wheels/reverse_geocoder-1.5.1-py3-none-any.whl` is reverse_geocoder 1.5.1 (https://github.com/thampiman/reverse-geocoder, by Ajay Thampi), built unmodified from its source package on PyPI; [deploy/dagster/wheels/README.md](deploy/dagster/wheels/README.md) says how, and how to check it.

- reverse_geocoder is licensed under the GNU Lesser General Public License, version 2.1. Its license text, as the upstream repository ships it, is [deploy/dagster/wheels/reverse_geocoder-LICENSE.txt](deploy/dagster/wheels/reverse_geocoder-LICENSE.txt). Its source is the PyPI source package that README names, and the upstream repository.
- The wheel's `rg_cities1000.csv` was derived by the reverse_geocoder project from the GeoNames cities1000 dataset (https://www.geonames.org/), which GeoNames licenses under the Creative Commons Attribution 4.0 International license (https://creativecommons.org/licenses/by/4.0/). It is shipped unmodified.
