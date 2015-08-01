require 'test_helper'

class StaticPagesControllerTest < ActionController::TestCase
  test "should get sign_up" do
    get :sign_up
    assert_response :success
  end

  test "should get advertising" do
    get :advertising
    assert_response :success
  end

  test "should get marketing" do
    get :marketing
    assert_response :success
  end

  test "should get social_impact" do
    get :social_impact
    assert_response :success
  end

end
