require 'json'

class StaticPagesController < ApplicationController
  def sign_up
  end

  def advertising
    data_root = File.expand_path(File.join(File.dirname(__FILE__),'../assets/data'))  
    # puts "data_root", data_root
    master_file = data_root+"/urbnearth.master.json"
    # puts "master_file", master_file
    @master_json = JSON.parse(File.read(master_file))
    #puts "master_json", @master_json
    
    @communities = []
    @master_json['advertising']['communities'].each do |profile_id|
      profile_json = JSON.parse(File.read(data_root+"/"+profile_id+".info.json"))
      profile_json['id'] = profile_json['id'].to_s
      @communities.push(profile_json)
    end
    
    @media = []
    @master_json['advertising']['media'].each do |profile_id|
      profile_json = JSON.parse(File.read(data_root+"/"+profile_id+".info.json"))
      profile_json['id'] = profile_json['id'].to_s
      @media.push(profile_json)
    end

    @celebrities = []
    @master_json['advertising']['celebrities'].each do |profile_id|
      profile_json = JSON.parse(File.read(data_root+"/"+profile_id+".info.json"))
      profile_json['id'] = profile_json['id'].to_s
      @celebrities.push(profile_json)
    end
    
  end

  def marketing
  end

  def social_impact
  end
end
