RPyC Release Process
====================

A walkthrough of doing a RPyC Release.

.. note::

   This describes the release process for the ``rpyc-async`` fork
   (``rr-develop/rpyc_async``). Unlike upstream RPyC, publishing is done
   through the ``Publish to PyPI`` GitHub Actions workflow using
   `Trusted Publishing <https://docs.pypi.org/trusted-publishers/>`_ (OIDC),
   so no API token is stored or passed on the command line.

1. Ensure a clean and current build environment (i.e., ``git pull; git status``)
2. Describe commit history within ``CHANGELOG.rst`` (see `Generate Entry`_)
3. Update ``release_date`` in ``rpyc_async/version.py`` and bump version (`Semantic Versioning`_ and `Versioning using Hatch`_)
4. Verify changes and run ``export ver=$(python -c 'import rpyc_async; print(rpyc_async.__version__)')``, ``git add .``, and ``git push``.
5. Mirror the private repository to the public one (``main``/``public`` branches) following the project's regular mirroring process.
6. Create an Annotated tag with a ``v`` prefix (the ``Publish to PyPI`` workflow triggers on ``v*`` tags): ``git tag -a v${ver} -m "Updated CHANGELOG.rst and version for release v${ver}"``
7. Publish release tag on the public repository: ``git push origin v${ver}``
8. The ``Publish to PyPI`` workflow builds the sdist/wheel and uploads it to PyPI via Trusted Publishing (OIDC). No token is needed; make sure the PyPI project has a pending publisher for ``rr-develop/rpyc_async`` with workflow ``Publish to PyPI`` (and environment ``pypi`` if configured).
9. Verify the release: in a clean environment run ``pip install rpyc-async==${ver}`` and ``python -c 'import rpyc_async; print(rpyc_async.__version__)'``.
10. Create a GitHub release such that the notes are from the ``CHANGELOG.rst`` entry (``%s/`#/#/g`` and ``%s/`_//g``), and you are done!

.. _Semantic Versioning: https://semver.org/
.. _Versioning using Hatch: https://hatch.pypa.io/latest/version/
.. _Build using Hatch: https://hatch.pypa.io/latest/build/
.. _Trusted Publishing: https://docs.pypi.org/trusted-publishers/


.. _Generate Entry:

Generate CHANGELOG.rst Entry
---------------------------------
To create an initial entry draft, run some shell commands.

.. code-block:: bash

    owner="rr-develop"
    repo="rpyc_async"
    #url="https://github.com/${owner}/${repo}"
    revisions="$(git rev-list $(pyenv exec hatch version)..HEAD | sed -z 's/\(.*\)\n/\1/;s/\n/|/g')"
    numbers=( $(git log $(pyenv exec hatch version)..HEAD --no-merges --oneline | sed -nE 's/^.*#([0-9]+).*/\1/p' | sort -nu) )
    issue_numbers="$(echo "${numbers[@]}" | sed 's/ /|/g')"
    #
    api_filter() { 
        jq -rc ".[] | select( .${1} | . != null) | select(.${1} | tostring | test(\"${2}\"))" "${3}"
    }
    url="https://api.github.com/repos/${owner}/${repo}"
    params="state=closed&accept=application/vnd.github+json"
    tmp_issues="/tmp/issues.json"
    tmp_pulls="/tmp/pulls.json"
    curl "${url}/issues?${params}" > "${tmp_issues}"
    curl "${url}/pulls?${params}" > "${tmp_pulls}"
    # Pulls
    gh_numbers=( )
    bullets=( )
    url_refs=( )
    while IFS= read -r pull; do
        title="$(echo "${pull}" | jq -r .title)"
        number="$(echo "${pull}" | jq -r .number)"
        pull_url="$(echo "${pull}" | jq -r .html_url)"
        # Add GH number
        gh_numbers+=( "${number}" )
        # Add bullet
        bullets+=( "- \`#${number}\`_ ${title}" )
        # Add url ref
        url_ref=".. _#${number}: ${pull_url}"
        url_refs+=( "${url_ref}" )
    done <<< "$(api_filter "merge_commit_sha" "${revisions}" "${tmp_pulls}")"
    # Issues
    while IFS= read -r issue; do
        title="$(echo "${issue}" | jq -r .title)"
        number="$(echo "${issue}" | jq -r .number)"
        issue_url="$(echo "${issue}" | jq -r .html_url)"
        # Add bullet
        bullets+=( "- \`#${number}\`_ ${title}" )
        # Add url ref
        url_ref=".. _#${number}: ${issue_url}"
        url_refs+=( "${url_ref}" )
    done <<< "$(api_filter "number" "${issue_numbers}" "${tmp_issues}")"

    # Header
    printf '5.X.Y\n=====\n'
    printf 'Date: %s\n\n' "$(date --rfc-3339=date)"
    for bullet in "${bullets[@]}"; do
        printf '%s\n' "${bullet}"
    done
    printf '\n'
    for ref in "${url_refs[@]}"; do
        printf '%s\n' "${ref}"
    done

Once insert this entry at the top of `CHANGELOG.rst`, review what it looks like with `instant-rst`.

.. code-block:: bash

    instantRst -b chromium -p 8612 -f "CHANGELOG.rst"


Misc. References
================
- `Wheel file name convention`_

.. _Wheel file name convention: https://peps.python.org/pep-0427/#file-name-convention
