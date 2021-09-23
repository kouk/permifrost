### Release Core
- [ ] Verify all necessary changes exist on the master branch
- [ ] Verify that the Changelog has correct, up to date changes for your release
- [ ] Check out permifrost repo.
- [ ] Create bumpversion branch from master branch.
- [ ] From root of directory run `bumpversion --new-version <new full version label> <new version part>`. eg: `bumpversion --new-version 0.12.0 minor`.
- [ ] Get this branch merged to ensure consistency with version numbering.
- [ ] Go to the tags section in repo: https://gitlab.com/gitlab-data/permifrost/-/tags and select new tag in the top right.
- [ ] Create a new tag with the new version number, make sure to follow the naming convention to get this tagged correctly in pypi: `v<new full version label`. eg:`v0.12.0`
- [ ] Ensure the pipelines for the new tag complete successfully, these should push the new release to Pypi.


### Announce
- [ ] Send out announcement in GitLab communications
- [ ] Announce in dbt Slack #tools-permifrost channel
