%global srcname mirrors-countme
%global pkgname mirrors_countme
%global modname countme

Name:    python-%{srcname}
Version: 0.0.4
Release: 1%{?dist}
Summary: access_log parsing & host counting for DNF mirrors
URL:     https://pagure.io/mirrors-countme
License: GPLv3+
Source0: https://pagure.io/%{srcname}/archive/%{version}/%{srcname}-%{version}.tar.gz
BuildArch: noarch
# Not quite sure what minimum sqlite version we need, but scripts use
# /usr/bin/sqlite3 and the python module is "sqlite3", so...
Requires: sqlite >= 3.0.0

%global _description %{expand:
A python module and scripts for parsing httpd access_log to find requests
including `countme=N`, parse the data included with those requests, and
compile weekly counts of DNF clients broken out by OS name, OS version,
system arch, etc.}

# This is for the toplevel metapackage.
%description %_description


# This section defines the python3-mirrors-countme subpackage.
%package -n python3-%{srcname}
Summary: %{summary}
BuildRequires: python3-devel python3-setuptools
#Recommends: python3-%%{srcname}+fancy_progress
# NOTE: in F33+ %%python_extras_subpkg can be used to automatically generate
# a 'python3-mirrors-countme+fancy_progress' subpackage that would pull in the
# expected requirements (i.e. tqdm). See the packaging docs for details on that:
# https://docs.fedoraproject.org/en-US/packaging-guidelines/Python/#_python_extras
# But since I'm targeting stuff older than F33 (and honestly this all feels
# like overkill anyway) I'm just gonna Recommend: tqdm here and call it good.
Recommends: %{py3_dist tqdm} >= 4.10.0

%description -n python3-%{srcname} %_description

%prep
%autosetup -n %{srcname}-%{version}

%build
%py3_build

%install
%py3_install

%check
%{python3} setup.py test

%files -n python3-%{srcname}
%license LICENSE.md
%doc README.md
%{python3_sitelib}/%{pkgname}-*.egg-info/
%{python3_sitelib}/%{modname}/
%{_bindir}/parse-access-log.py
%{_bindir}/countme-totals.py
%{_bindir}/countme-update-rawdb.sh
%{_bindir}/countme-update-totals.sh
%{_bindir}/countme-csv2sqlite.sh
%{_bindir}/countme-sqlite2csv.sh
