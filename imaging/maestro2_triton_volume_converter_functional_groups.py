import pydicom


def shared_functional_groups_sequence(dataset, x):
    """
    Create the shared functional group sequence in the dataset.

    This function constructs the shared functional group sequence for a DICOM dataset,
    populating it with anatomical, reference, orientation, and pixel measurement information.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the functional groups are added.
        x (list): List containing data for constructing the functional groups.
    """

    purpose_of_reference_code_seq = pydicom.Sequence()
    purpose_of_reference_code_item = pydicom.Dataset()
    purpose_of_reference_code_item.CodeValue = (
        x[0]["52009229"]
        .value[0]["00081140"]
        .value[0]["0040A170"]
        .value[0]["00080100"]
        .value
    )
    purpose_of_reference_code_item.CodingSchemeDesignator = (
        x[0]["52009229"]
        .value[0]["00081140"]
        .value[0]["0040A170"]
        .value[0]["00080102"]
        .value
    )
    purpose_of_reference_code_item.CodeMeaning = (
        x[0]["52009229"]
        .value[0]["00081140"]
        .value[0]["0040A170"]
        .value[0]["00080104"]
        .value
    )
    purpose_of_reference_code_seq.append(purpose_of_reference_code_item)

    referenced_image_seq = pydicom.Sequence()
    referenced_image_item = pydicom.Dataset()
    referenced_image_item.ReferencedSOPClassUID = (
        x[0]["52009229"].value[0]["00081140"].value[0]["00081150"].value
    )
    referenced_image_item.ReferencedSOPInstanceUID = (
        x[0]["52009229"].value[0]["00081140"].value[0]["00081155"].value
    )
    referenced_image_item.PurposeOfReferenceCodeSequence = purpose_of_reference_code_seq

    referenced_image_seq.append(referenced_image_item)

    anatomic_region_seq = pydicom.Sequence()
    anatomic_region_item = pydicom.Dataset()

    anatomic_region_item.CodeValue = (
        x[0]["52009229"]
        .value[0]["00209071"]
        .value[0]["00082218"]
        .value[0]["00080100"]
        .value
    )
    anatomic_region_item.CodingSchemeDesignator = (
        x[0]["52009229"]
        .value[0]["00209071"]
        .value[0]["00082218"]
        .value[0]["00080102"]
        .value
    )
    anatomic_region_item.CodeMeaning = (
        x[0]["52009229"]
        .value[0]["00209071"]
        .value[0]["00082218"]
        .value[0]["00080104"]
        .value
    )
    anatomic_region_seq.append(anatomic_region_item)

    frame_anatomy_seq = pydicom.Sequence()
    frame_anatomy_item = pydicom.Dataset()

    frame_anatomy_item.AnatomicRegionSequence = anatomic_region_seq
    frame_anatomy_item.FrameLaterality = (
        x[0]["52009229"].value[0]["00209071"].value[0]["00209072"].value
    )

    frame_anatomy_seq.append(frame_anatomy_item)

    plane_orientation_seq = pydicom.Sequence()
    plane_orientation_item = pydicom.Dataset()
    plane_orientation_item.ImageOrientation = (
        x[0]["52009229"].value[0]["00209116"].value[0]["00200037"].value
    )
    plane_orientation_seq.append(plane_orientation_item)

    pixel_measures_seq = pydicom.Sequence()
    pixel_measures_item = pydicom.Dataset()

    pixel_measures_item.SliceThickness = (
        x[0]["52009229"].value[0]["00289110"].value[0]["00180050"].value
    )
    pixel_measures_item.PixelSpacing = (
        x[0]["52009229"].value[0]["00289110"].value[0]["00280030"].value
    )

    pixel_measures_seq.append(pixel_measures_item)

    frame_voi_lut_seq = pydicom.Sequence()
    frame_voi_lut_item = pydicom.Dataset()

    frame_voi_lut_item.WindowCenter = (
        x[0]["52009229"].value[0]["00289132"].value[0]["00281050"].value
    )
    frame_voi_lut_item.WindowWidth = (
        x[0]["52009229"].value[0]["00289132"].value[0]["00281051"].value
    )
    frame_voi_lut_item.VOILUTFunction = (
        x[0]["52009229"].value[0]["00289132"].value[0]["00281056"].value
    )

    frame_voi_lut_seq.append(frame_voi_lut_item)

    shared_functional_groups_seq = pydicom.Sequence()
    dataset.SharedFunctionalGroupsSequence = shared_functional_groups_seq
    shared_functional_groups_item = pydicom.Dataset()

    shared_functional_groups_item.ReferencedImageSequence = referenced_image_seq
    shared_functional_groups_item.FrameAnatomySequence = frame_anatomy_seq
    shared_functional_groups_item.PlaneOrientationSequence = plane_orientation_seq
    shared_functional_groups_item.PixelMeasuresSequence = pixel_measures_seq
    shared_functional_groups_item.FrameVOILUTSequence = frame_voi_lut_seq

    shared_functional_groups_seq.append(shared_functional_groups_item)


def acquisition_method_algorithm_sequence(dataset, x):
    """
    Create the acquisition method algorithm sequence in the dataset.

    This function constructs the acquisition method algorithm sequence for a DICOM dataset,
    populating it with algorithm family code sequence, algorithm version, and algorithm name.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the acquisition method algorithm is added.
        x (list): List containing data for constructing the acquisition method algorithm.
    """

    algorithm_family_code_seq = pydicom.Sequence()
    algorithm_family_code_item = pydicom.Dataset()

    algorithm_family_code_item.CodeValue = (
        x[0]["00221423"].value[0]["0066002F"].value[0]["00080100"].value
    )
    algorithm_family_code_item.CodingSchemeDesignator = (
        x[0]["00221423"].value[0]["0066002F"].value[0]["00080102"].value
    )
    algorithm_family_code_item.CodeMeaning = (
        x[0]["00221423"].value[0]["0066002F"].value[0]["00080104"].value
    )
    algorithm_family_code_seq.append(algorithm_family_code_item)

    acquisition_method_algorithm_seq = pydicom.Sequence()
    dataset.AcquisitionMethodAlgorithmSequence = acquisition_method_algorithm_seq
    acquisition_method_algorithm_item = pydicom.Dataset()

    acquisition_method_algorithm_item.AlgorithmFamilyCodeSequence = (
        algorithm_family_code_seq
    )
    acquisition_method_algorithm_item.AlgorithmVersion = (
        x[0]["00221423"].value[0]["00660031"].value
    )
    acquisition_method_algorithm_item.AlgorithmName = (
        x[0]["00221423"].value[0]["00660036"].value
    )

    acquisition_method_algorithm_seq.append(acquisition_method_algorithm_item)


def octb_scan_analysis_acquisition_parameters_sequence(dataset, x):
    """
    Create the OCTB scan analysis acquisition parameters sequence in the dataset.

    This function constructs the OCTB scan analysis acquisition parameters sequence for a DICOM dataset,
    populating it with scan pattern type code sequence, number of B-scans per frame, B-scan slab thickness,
    distance between B-scan slabs, B-scan cycle time, and A-scan rate.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the OCTB scan analysis acquisition parameters are added.
        x (list): List containing data for constructing the OCTB scan analysis acquisition parameters.
    """

    scan_pattern_type_code_seq = pydicom.Sequence()
    scan_pattern_type_code_item = pydicom.Dataset()

    scan_pattern_type_code_item.CodeValue = (
        x[0]["00221640"].value[0]["00221618"].value[0]["00080100"].value
    )
    scan_pattern_type_code_item.CodingSchemeDesignator = (
        x[0]["00221640"].value[0]["00221618"].value[0]["00080102"].value
    )
    scan_pattern_type_code_item.CodeMeaning = (
        x[0]["00221640"].value[0]["00221618"].value[0]["00080104"].value
    )
    scan_pattern_type_code_seq.append(scan_pattern_type_code_item)

    octb_scan_analysis_acquisition_parameters_seq = pydicom.Sequence()
    dataset.OCTBscanAnalysisAcquisitionParametersSequence = (
        octb_scan_analysis_acquisition_parameters_seq
    )
    octb_scan_analysis_acquisition_parameters_item = pydicom.Dataset()

    octb_scan_analysis_acquisition_parameters_item.NumberOfBscansPerFrame = (
        x[0]["00221640"].value[0]["00221642"].value
    )
    octb_scan_analysis_acquisition_parameters_item.BscanSlabThickness = (
        x[0]["00221640"].value[0]["00221643"].value
    )
    octb_scan_analysis_acquisition_parameters_item.DistanceBetweenBscanSlabs = (
        x[0]["00221640"].value[0]["00221644"].value
    )
    octb_scan_analysis_acquisition_parameters_item.BscanCycleTime = (
        x[0]["00221640"].value[0]["00221645"].value
    )
    octb_scan_analysis_acquisition_parameters_item.AscanRate = (
        x[0]["00221640"].value[0]["00221649"].value
    )
    octb_scan_analysis_acquisition_parameters_item.ScanPatternTypeCodeSequence = (
        scan_pattern_type_code_seq
    )

    octb_scan_analysis_acquisition_parameters_seq.append(
        octb_scan_analysis_acquisition_parameters_item
    )


def per_frame_functional_groups_sequence(dataset, x):
    """
    Create the per-frame functional groups sequence in the dataset.

    This function constructs the per-frame functional groups sequence for a DICOM dataset,
    populating it with per-frame content, reference, ophthalmic, and position information.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the functional groups are added.
        x (list): List containing data for constructing the functional groups.
    """
    per_frame_functional_groups_seq = pydicom.Sequence()
    repeat = len(x[0]["52009230"].value)

    for i in range(repeat):

        plane_position_seq = pydicom.Sequence()
        plane_position_item = pydicom.Dataset()
        plane_position_item.ImagePosition = (
            x[0]["52009230"].value[i]["00209113"].value[0]["00200032"].value
        )
        plane_position_seq.append(plane_position_item)

        purpose_of_reference_seq1 = pydicom.Sequence()
        purpose_of_reference_item1 = pydicom.Dataset()
        purpose_of_reference_item1.CodeValue = (
            x[0]["52009230"]
            .value[i]["00089124"]
            .value[0]["00082112"]
            .value[0]["0040A170"]
            .value[0]["00080100"]
            .value
        )
        purpose_of_reference_item1.CodingSchemeDesignator = (
            x[0]["52009230"]
            .value[i]["00089124"]
            .value[0]["00082112"]
            .value[0]["0040A170"]
            .value[0]["00080102"]
            .value
        )
        purpose_of_reference_item1.CodeMeaning = (
            x[0]["52009230"]
            .value[i]["00089124"]
            .value[0]["00082112"]
            .value[0]["0040A170"]
            .value[0]["00080104"]
            .value
        )
        purpose_of_reference_seq1.append(purpose_of_reference_item1)

        ##change reference
        source_image_seq = pydicom.Sequence()
        source_image_item = pydicom.Dataset()
        source_image_item.ReferencedSOPClassUID = (
            x[0]["52009230"]
            .value[i]["00089124"]
            .value[0]["00082112"]
            .value[0]["00081150"]
            .value
        )
        source_image_item.ReferencedSOPInstanceUID = (
            x[0]["52009230"]
            .value[i]["00089124"]
            .value[0]["00082112"]
            .value[0]["00081155"]
            .value
        )
        source_image_item.ReferencedFrameNumber = (
            x[0]["52009230"]
            .value[i]["00089124"]
            .value[0]["00082112"]
            .value[0]["00081160"]
            .value
        )
        source_image_item.SpatialLocationsPreserved = (
            x[0]["52009230"]
            .value[i]["00089124"]
            .value[0]["00082112"]
            .value[0]["0028135A"]
            .value
        )
        source_image_item.PurposeOfReferenceCodeSequence = purpose_of_reference_seq1
        source_image_seq.append(source_image_item)

        derivation_code_seq = pydicom.Sequence()
        derivation_code_item = pydicom.Dataset()
        derivation_code_item.CodeValue = (
            x[0]["52009230"]
            .value[i]["00089124"]
            .value[0]["00089215"]
            .value[0]["00080100"]
            .value
        )
        derivation_code_item.CodingSchemeDesignator = (
            x[0]["52009230"]
            .value[i]["00089124"]
            .value[0]["00089215"]
            .value[0]["00080102"]
            .value
        )
        derivation_code_item.CodeMeaning = (
            x[0]["52009230"]
            .value[i]["00089124"]
            .value[0]["00089215"]
            .value[0]["00080104"]
            .value
        )
        derivation_code_seq.append(derivation_code_item)

        derivation_image_seq = pydicom.Sequence()
        derivation_image_item = pydicom.Dataset()
        derivation_image_item.SourceImageSequence = source_image_seq
        derivation_image_item.DerivationCodeSequence = derivation_code_seq

        derivation_image_seq.append(derivation_image_item)

        frame_content_seq = pydicom.Sequence()
        frame_content_item = pydicom.Dataset()

        frame_content_item.FrameAcquisitionDateTime = (
            x[0]["52009230"].value[i]["00209111"].value[0]["00189074"].value
        )
        frame_content_item.FrameReferenceDateTime = (
            x[0]["52009230"].value[i]["00209111"].value[0]["00189151"].value
        )
        frame_content_item.FrameAcquisitionDuration = (
            x[0]["52009230"].value[i]["00209111"].value[0]["00189220"].value
        )
        frame_content_item.StackID = (
            x[0]["52009230"].value[i]["00209111"].value[0]["00209056"].value
        )
        frame_content_item.InStackPositionNumber = (
            x[0]["52009230"].value[i]["00209111"].value[0]["00209057"].value
        )
        frame_content_item.DimensionIndexValues = (
            x[0]["52009230"].value[i]["00209111"].value[0]["00209157"].value
        )

        frame_content_seq.append(frame_content_item)

        per_frame_functional_groups_item = pydicom.Dataset()

        per_frame_functional_groups_item.FrameContentSequence = frame_content_seq
        per_frame_functional_groups_item.PlanePositionSequence = plane_position_seq
        per_frame_functional_groups_item.DerivationImageSequence = derivation_image_seq

        per_frame_functional_groups_seq.append(per_frame_functional_groups_item)

    dataset.PerFrameFunctionalGroupsSequence = per_frame_functional_groups_seq


def dimension_organization_sequence(dataset, x):
    """
    Create the dimension organization sequence in the dataset.

    This function constructs the dimension organization sequence for a DICOM dataset,
    populating it with the dimension organization UID.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the dimension organization is added.
        x (list): List containing data for constructing the dimension organization.
    """

    dimension_organization_seq = pydicom.Sequence()
    dimension_organization_item = pydicom.Dataset()

    dimension_organization_item.DimensionOrganizationUID = (
        x[0]["00209221"].value[0]["00209164"].value
    )

    dimension_organization_seq.append(dimension_organization_item)

    dataset.DimensionOrganizationSequence = dimension_organization_seq


def dimension_index_sequence(dataset, x):
    """
    Create the dimension index sequence in the dataset.

    This function constructs the dimension index sequence for a DICOM dataset,
    populating it with the dimension organization UID, dimension index pointer,
    and functional group pointer.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the dimension index is added.
        x (list): List containing data for constructing the dimension index.
    """

    dimension_index_seq = pydicom.Sequence()
    dimension_index_item = pydicom.Dataset()

    dimension_index_item.DimensionOrganizationUID = (
        x[0]["00209221"].value[0]["00209164"].value
    )
    dimension_index_item.DimensionIndexPointer = "00209056"
    dimension_index_item.FunctionalGroupPointer = "00209111"

    dimension_index_seq.append(dimension_index_item)

    dimension_index_item1 = pydicom.Dataset()

    dimension_index_item1.DimensionOrganizationUID = (
        x[0]["00209221"].value[0]["00209164"].value
    )
    dimension_index_item1.DimensionIndexPointer = "00209057"
    dimension_index_item1.FunctionalGroupPointer = "00209111"

    dimension_index_seq.append(dimension_index_item1)

    dataset.DimensionIndexSequence = dimension_index_seq
