require File.expand_path('../lib/mssql-kissmetrics/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Drew Gillson"]
  gem.email         = ["drew.gillson@gmail.com"]
  gem.description   = %q{Quickly load historic purchase behavior from Magento into KISSMetrics}
  gem.summary       = %q{Integrate Magento and KISSMetrics}
  gem.homepage      = "https://github.com/drewgillson/mssql-kissmetrics"
  gem.license       = 'MIT'

  gem.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.files         = `git ls-files`.split("\n")
  gem.name          = "mssql-kissmetrics"
  gem.version       = MssqlKissmetrics::VERSION
end
