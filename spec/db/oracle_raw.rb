begin 
  oracle_connection
rescue Exception => e
  oracle_enhanced_connection
end

