Rails.application.routes.draw do

  root 			      'static_pages#sign_up'
  get 'advertising'	      => 'static_pages#advertising'
  get 'marketing'	      => 'static_pages#marketing'
  get 'social_impact'	      => 'static_pages#social_impact'
end
