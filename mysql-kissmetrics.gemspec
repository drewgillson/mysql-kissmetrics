require File.expand_path('../lib/mysql-kissmetrics/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Drew Gillson"]
  gem.email         = ["drew.gillson@gmail.com"]
  gem.description   = %q{Quickly load historic behavior from Magento into KISSMetrics}
  gem.summary       = %q{Integrate Magento's MySQL backend with KISSMetrics}
  gem.homepage      = "https://github.com/drewgillson/mssql-kissmetrics"
  gem.license       = 'MIT'

  gem.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.files         = `git ls-files`.split("\n")
  gem.name          = "mysql-kissmetrics"
  gem.version       = MysqlKissmetrics::VERSION
end
