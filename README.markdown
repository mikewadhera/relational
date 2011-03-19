# relational

a modern, lightweight mysql ORM for ruby 1.9

## Features

### modern
- uses ARel for Query API, MySQL2 EM fiber-enabled driver for asynchronous I/O
- supports partial updates and dirty tracking
	
### lightweight
- zero-copy attribute loading
- small runtime footprint

### non-blocking
- designed for use with Rack::FiberPool for transparent, non-blocking access to mysql without need for callbacks/CPS
	
## Non-Features

- validations (you shouldn't be encoding your business logic in your persistence layer anyway)
- migrations - ActiveRecord::Migrations is a fantastic migration toolkit - use it!

