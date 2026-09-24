package org.example.ptit_cntt1_it214_session18_mini.order.feign;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProductDto implements Serializable {
    private Long id;
    private String code;
    private String name;
    private BigDecimal price;
    private Integer stockQuantity;
    private String description;
}
