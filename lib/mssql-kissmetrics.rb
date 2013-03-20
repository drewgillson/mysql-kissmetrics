require "mssql-kissmetrics/version"

module MssqlKissmetrics
    
    require 'dbi'
    require 'km'
    require 'date'

    def self.initialize(profile, username, password, km_key, allowed_history_days = 0)
        @allowed_history_days = allowed_history_days
        @now = DateTime.now.to_time.to_i

        KM.init(km_key, :log_dir => 'log/')

        conn = DBI.connect("dbi:ODBC:" << profile, username, password) do |dbh|
            sth = dbh.execute("SELECT a.entity_id, DATE_FORMAT(DATE_ADD(a.created_at, INTERVAL -7 HOUR),'%b %d %Y %h:%i %p') AS created_at, a.customer_email, a.customer_firstname, a.customer_lastname, a.increment_id, a.grand_total, a.subtotal, b.region, b.postcode, b.city, a.coupon_code
                                       FROM sales_flat_order AS a
                                       INNER JOIN sales_flat_order_address AS b ON a.shipping_address_id = b.entity_id AND b.address_type = 'shipping' " <<
                                       (@allowed_history_days > 0 ? "WHERE  " << @now.to_s << " - UNIX_TIMESTAMP(DATE_ADD(a.created_at, INTERVAL -7 HOUR) ) <= " << (@allowed_history_days * 86000).to_s << " " : "") <<
                                       "ORDER BY increment_id DESC")
            while row = sth.fetch do
                KM.identify(row['customer_email'])

                KM.set({"First Name" => row['customer_firstname'],
                              "Last Name" => row['customer_lastname'],
                              "Province" => row['region'], 
                              "Postal Code" => row['postcode'],
                              "City" => row['city']})

                ts = DateTime.parse(row['created_at']).to_time.to_i
                KM.record('Purchased', {'Order ID' => row['increment_id'].to_i,
                                                          'Order Total' => row['grand_total'].to_f,
                                                          'Order Subtotal' => row['subtotal'].to_f,
                                                          'Ship to Province' => row['region'],
                                                          'Coupon Code' => row['coupon_code'],
                                                          '_d' => 1,
                                                          '_t' => ts})
                xth = dbh.execute("SELECT b.sku, k.value AS brand, d.value AS style, j.value AS season, i.value AS product, n.value AS color, o.value AS size, (SELECT MAX(price) FROM sales_flat_order_item WHERE order_id = b.order_id AND sku = b.sku) AS price
                                                FROM sales_flat_order AS a
                                                INNER JOIN sales_flat_order_item AS b ON a.entity_id = b.order_id AND product_type = 'simple'
                                                LEFT JOIN catalog_product_entity_int AS c ON b.product_id = c.entity_id AND c.attribute_id = (SELECT attribute_id FROM eav_attribute WHERE attribute_code = 'manufacturer' AND entity_type_id = 4)
                                                LEFT JOIN catalog_product_entity_varchar AS d ON b.product_id = d.entity_id AND d.attribute_id = (SELECT attribute_id FROM eav_attribute WHERE attribute_code = 'vendor_product_id' AND entity_type_id = 4)
                                                LEFT JOIN catalog_product_entity_int AS f ON b.product_id = f.entity_id AND f.attribute_id = (SELECT attribute_id FROM eav_attribute WHERE attribute_code = 'season_id' AND entity_type_id = 4)
                                                LEFT JOIN catalog_product_entity_varchar AS i ON b.product_id = i.entity_id AND i.attribute_id = (SELECT attribute_id FROM eav_attribute WHERE attribute_code = 'name' AND entity_type_id = 4)
                                                LEFT JOIN eav_attribute_option_value AS j ON f.value = j.option_id AND j.store_id = 0
                                                LEFT JOIN eav_attribute_option_value AS k ON c.value = k.option_id AND k.store_id = 0
                                                LEFT JOIN catalog_product_entity_int AS l ON b.product_id = l.entity_id AND l.attribute_id = (SELECT attribute_id FROM eav_attribute WHERE attribute_code = 'choose_color' AND entity_type_id = 4)
                                                LEFT JOIN catalog_product_entity_int AS m ON b.product_id = m.entity_id AND m.attribute_id = (SELECT attribute_id FROM eav_attribute WHERE attribute_code = 'choose_size' AND entity_type_id = 4)
                                                LEFT JOIN eav_attribute_option_value AS n ON l.value = n.option_id AND n.store_id = 0
                                                LEFT JOIN eav_attribute_option_value AS o ON m.value = o.option_id AND o.store_id = 0
                                                WHERE a.entity_id = " << row['entity_id'].to_s)
                while item = xth.fetch do
                    ts = ts + 1

                    if item['brand'] != "" && item['brand'] != nil
                        KM.set({"Brand" => item['brand'],
                                      "Product" => item['product'],
                                      "Color" => item['color'],
                                      "Size" => item['size'],
                                      "Season" => item['season'],
                                      "Style" => item['style'],
                                      "Price" => item['price'].to_f,
                                      "_t" => ts,
                                      "_d" => 1})
                    end
                end
                xth.finish
            end
            sth.finish
        end
        conn.disconnect
    end
end