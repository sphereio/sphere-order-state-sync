# sphere-order-state-sync
============================

[![Build Status](https://travis-ci.org/sphereio/sphere-order-state-sync.png?branch=master)](https://travis-ci.org/sphereio/sphere-order-state-sync) [![Coverage Status](https://coveralls.io/repos/sphereio/sphere-order-state-sync/badge.png?branch=master)](https://coveralls.io/r/sphereio/sphere-order-state-sync?branch=master) [![Dependency Status](https://david-dm.org/sphereio/sphere-order-state-sync.png?theme=shields.io)](https://david-dm.org/sphereio/sphere-order-state-sync) [![devDependency Status](https://david-dm.org/sphereio/sphere-order-state-sync/dev-status.png?theme=shields.io)](https://david-dm.org/sphereio/sphere-order-state-sync#info=devDependencies)

Service listens for line item state change messages in some project and replicates them to another SPHERE.IO project.

## Getting Started

Install the module with: `npm install sphere-order-state-sync`

## Setup

* create `config.js`
  * make `create_config.sh`executable

    ```
    chmod +x create_config.sh
    ```
  * run script to generate `config.js`

    ```
    ./create_config.sh
    ```
* configure github/hipchat integration (see project *settings* in guthub)
* install travis gem `gem install travis`
* add encrpyted keys to `.travis.yml`
 * add sphere project credentials to `.travis.yml`

        ```
        travis encrypt [xxx] --add SPHERE_PROJECT_KEY
        travis encrypt [xxx] --add SPHERE_CLIENT_ID
        travis encrypt [xxx] --add SPHERE_CLIENT_SECRET
        ```
  * add hipchat credentials to `.travis.yml`

        ```
        travis encrypt [xxx]@Sphere --add notifications.hipchat.rooms
        ```

## Documentation
_(Coming soon)_

## Tests
Tests are written using [jasmine](https://jasmine.github.io/) (behavior-driven development framework for testing javascript code). Thanks to [jasmine-node](https://github.com/mhevery/jasmine-node), this test framework is also available for node.js.

To run tests, simple execute the *test* task using `grunt`.
```bash
$ grunt test
```

## Examples
_(Coming soon)_

## Contributing
In lieu of a formal styleguide, take care to maintain the existing coding style. Add unit tests for any new or changed functionality. Lint and test your code using [Grunt](http://gruntjs.com/).
More info [here](CONTRIBUTING.md)

## Releasing
Releasing a new version is completely automated using the Grunt task `grunt release`.

```javascript
grunt release // patch release
grunt release:minor // minor release
grunt release:major // major release
```

## Styleguide
We <3 CoffeeScript here at commercetools! So please have a look at this referenced [coffeescript styleguide](https://github.com/polarmobile/coffeescript-style-guide) when doing changes to the code.

## License
Copyright (c) 2014 Oleg Ilyenko
Licensed under the MIT license.
