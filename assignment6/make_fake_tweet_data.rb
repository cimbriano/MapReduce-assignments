require 'randexp'
require 'faker'

File.open("tweets_local.txt", 'w') do |file|
	100.times do

		# Ugly way to make a tweet id from 5 random ints up to 100
		tweet_id = Array.new(5){rand 100}.map {|e| e.to_s}.inject(""){ |acc, ele| acc << ele }
		date = "#{/(Sun|Mon|Tue|Wed|Thu|Fri|Sat)/.gen} #{/(Jan|Feb)/.gen} #{1 + (rand 31)} #{rand 23}:#{rand 60}:#{rand 60} +0000 2011"
		user_name = Faker::Name.last_name.downcase
		tweet_text = Faker::Lorem.sentence

		file.write(tweet_id + "\t")
		file.write(date + "\t")
		file.write(user_name + "\t")
		file.write(tweet_text)
		file.write("\n")

	end	
end